import os
import sys
import re
import pandas as pd
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
)

TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 344854611 # ← вставь свой Telegram user_id

user_sessions = {}

def calculate_percentiles_and_median(series):
    return {'95th_percentile': series.quantile(0.95),
            'median': series.median()
           }

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Привет! Используй /batch, чтобы загрузить файлы. Потом — /done.")

async def batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_sessions[user_id] = []
    os.makedirs(f"temp/{user_id}", exist_ok=True)
    await update.message.reply_text("📥 Жду загрузку CSV-файлов. Когда закончишь — напиши /done.")

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    file_list = user_sessions.get(user_id, [])

    if not file_list:
        await update.message.reply_text("😕 Ты не загрузил ни одного файла.")
        return

    await update.message.reply_text("🔍 Обрабатываю файлы по отдельности...")

    try:
        for path in file_list:
            filename = os.path.basename(path)

            # Назначаем имя датафрейма
            if filename.lower().startswith("transaction"):
                df_name = "df_upgate"
            elif re.match(r"^\d{8}_\d{6}", filename):
                df_name = "df_unlimit"
            elif filename.lower().startswith("report"):
                df_name = "df_payabl"
            elif filename.lower().startswith("export"):
                df_name = "df_centrobill"
            else:
                df_name = "df_unknown"

            # Специальная обработка для df_upgate
            if df_name == "df_upgate":
                df = pd.read_csv(path)
                upgate = df.copy() 
                upgate = upgate.dropna(subset=['id']).copy()
                upgate['createdAt'] = pd.to_datetime(upgate['createdAt'], errors='coerce', utc=True)
                upgate['payment.createdAt'] = pd.to_datetime(upgate['payment.createdAt'], errors='coerce', utc=True)
                # 1. Поиск массовых попыток платежей с одного Email
                df_ip_counts_upgate = upgate.groupby('payment.email')['operationId'].nunique()
                upgate['email_operation_count'] = upgate['payment.email'].map(df_ip_counts_upgate)
                
                # 2. Несоответствие страны платежа и IP-адреса (исключая NaN)
                upgate['geo_mismatch'] = (upgate['payment.countryCode'].notna() & upgate['paymentContext.IP_COUNTRY_CODE'].notna() & 
                                      (upgate['payment.countryCode'] != upgate['paymentContext.IP_COUNTRY_CODE']))
                
                # 3. Подсчет количества разных email для одной карты
                card_bin_email_counts_upgate = upgate.groupby(['paymentDetails.CARD_BIN', 'paymentDetails.CARD_LAST_FOUR_DIGITS'])['payment.email'].nunique()
                upgate['card_used_by_different_emails'] = upgate.set_index(['paymentDetails.CARD_BIN', 'paymentDetails.CARD_LAST_FOUR_DIGITS']).index.map(card_bin_email_counts_upgate)
                
                # 4. Подсчет количества разных карт для одного email
                df_email_card_counts_upgate = upgate.groupby('payment.email')['paymentDetails.CARD_LAST_FOUR_DIGITS'].nunique()
                upgate['different_cards_per_email'] = upgate['payment.email'].map(df_email_card_counts_upgate)
                
                # 5. Несоответствие User-Agent у одного аккаунта
                df_ua_counts_upgate = upgate.groupby('payment.email')['paymentContext.BROWSER_USER_AGENT'].nunique()
                upgate['ua_variety'] = upgate['payment.email'].map(df_ua_counts_upgate)
                
                # 6. Подозрительные попытки 3DS-аутентификации
                failed_3ds_upgate = upgate[upgate["transactionDetails.THREE_DS_STATUS"].isin(["N", "R", "U"])]
                
                failed_3ds_email_counts_upgate = failed_3ds_upgate["payment.email"].value_counts()
                upgate["failed_3ds_per_email"] = upgate["payment.email"].map(failed_3ds_email_counts_upgate).fillna(0).astype(int)
                
                failed_3ds_ip_counts_upgate = failed_3ds_upgate["paymentContext.IP"].value_counts()
                upgate["failed_3ds_per_ip"] = upgate["paymentContext.IP"].map(failed_3ds_ip_counts_upgate).fillna(0).astype(int)
                
                # 7. Мультиаккаунтинг (один IP на много аккаунтов)
                df_multiacc_upgate = upgate.groupby('paymentContext.IP')['payment.email'].nunique()
                upgate['multiacc'] = upgate['paymentContext.IP'].map(df_multiacc_upgate)
                
                # 8. Количество уникальных BIN на email
                upgate["paymentDetails.CARD_BIN"] = upgate["paymentDetails.CARD_BIN"].astype(str)
                bin_per_email_upgate = upgate.groupby("payment.email")["paymentDetails.CARD_BIN"].nunique()
                upgate["unique_bins_per_email"] = upgate["payment.email"].map(bin_per_email_upgate).fillna(0).astype(int)
                
                # 9. Количество уникальных имен владельцев карт на email
                names_per_email_upgate = upgate.groupby("payment.email")["cardData.cardFullName"].nunique()
                upgate["unique_card_names_per_email"] = upgate["payment.email"].map(names_per_email_upgate).fillna(0).astype(int)
                
                # 10. Количество email на одного владельца карты
                emails_per_card_name_upgate = upgate.groupby("cardData.cardFullName")["payment.email"].nunique()
                upgate["unique_emails_per_card_name"] = upgate["cardData.cardFullName"].map(emails_per_card_name_upgate).fillna(0).astype(int)
                
                # 11. Выявление неестественно больших сумм
                upgate['large_payment'] = upgate['payment.amount'] > upgate['payment.amount'].quantile(0.95)
                
                # 12. Подсчет успешных и неуспешных транзакций
                successful_transactions_upgate = upgate[(upgate['transactionType'] == 'SALE') & (upgate['responseCodeStatus'] == 'SUCCESS')]
                unsuccessful_transactions_upgate = upgate[(upgate['transactionType'] == 'SALE') & (upgate['responseCodeStatus'] == 'DECLINE')]
                fraud_related_transactions_upgate = upgate[upgate['transactionType'].isin(['FRAUD_ALERT', 'CHARGEBACK'])]
                
                # 13. Учет fraud_related_transactions
                fraud_transactions_count_upgate = fraud_related_transactions_upgate.groupby('payment.email').size()
                upgate['fraud_transactions_count'] = upgate['payment.email'].map(fraud_transactions_count_upgate).fillna(0)
                
                # 14. Расчет отношения неуспешных транзакций к успешным
                failure_ratio_upgate = unsuccessful_transactions_upgate.groupby('payment.email').size() / successful_transactions_upgate.groupby('payment.email').size()
                upgate['failure_ratio'] = upgate['payment.email'].map(failure_ratio_upgate).fillna(0)
                
                stats_dict_upgate = {col: calculate_percentiles_and_median(upgate[col]) for col in [
                    'email_operation_count', 
                    'card_used_by_different_emails', 
                    'different_cards_per_email', 
                    'ua_variety',
                    'failed_3ds_per_email', 
                    'multiacc', 
                    'unique_bins_per_email',
                    'unique_card_names_per_email', 
                    'unique_emails_per_card_name', 
                    'failure_ratio', 
                    'fraud_transactions_count'
                ]}
                
                stats_upgate = pd.DataFrame(stats_dict_upgate).T
                
                # Применение порогов
                df_stats_upgate = upgate.copy()
                for col in stats_dict_upgate.keys():
                    threshold_upgate = stats_upgate.loc[col, '95th_percentile']
                    df_stats_upgate[f'is_fraud_{col}'] = df_stats_upgate[col] > threshold_upgate
                
                # Флаг для geo_mismatch
                df_stats_upgate['is_fraud_geo_mismatch'] = df_stats_upgate['geo_mismatch']
                
                # Флаг для больших сумм
                df_stats_upgate['is_fraud_large_payment'] = df_stats_upgate['large_payment']
                
                weights_upgate = {
                    'is_fraud_failed_3ds_per_email': 0.25,
                    'is_fraud_email_operation_count': 0.1,
                    'is_fraud_different_cards_per_email': 0.7,
                    'is_fraud_card_used_by_different_emails': 0.25,
                    'is_fraud_geo_mismatch': 0.1,
                    'is_fraud_ua_variety': 0.15,
                    'is_fraud_unique_bins_per_email': 0.25,
                    'is_fraud_unique_card_names_per_email': 0.2,
                    'is_fraud_unique_emails_per_card_name': 0.25,
                    'is_fraud_large_payment': 0.1,
                    'is_fraud_failure_ratio': 0.15,
                    'is_fraud_fraud_transactions_count': 1
                }
                
                # Рассчитываем суммарный вес
                total_weight_upgate = sum(weights_upgate.values())
                
                # Группировка по email для вычисления суммарных флагов
                df_unique_emails_upgate = df_stats_upgate.drop_duplicates(subset='payment.email')
                df_user_stats_upgate = df_unique_emails_upgate.groupby('payment.email')[list(weights_upgate.keys())].sum()
                
                # Подсчет итогового fraud_score с нормализацией
                for col, weight in weights_upgate.items():
                    df_user_stats_upgate[col] *= weight
                
                df_user_stats_upgate['fraud_score_upgate'] = df_user_stats_upgate[list(weights_upgate.keys())].sum(axis=1)
                
                # Сортировка по fraud_score
                fraud_users_sorted_upgate = df_user_stats_upgate.sort_values(by='fraud_score_upgate', ascending=False)

                fraud_users_sorted_upgate = fraud_users_sorted_upgate.reset_index()
                
                # Выводим результат
                df = fraud_users_sorted_upgate.copy()
                
            elif df_name == "df_unlimit":
                df = pd.read_csv(path, sep=";")
                
                unlimit = df.copy()
                
                unlimit = unlimit[unlimit['Card type'] != 'ewallet'].copy()

                # 1. Поиск массовых попыток платежей с одного Email
                df_ip_counts_unlimit = unlimit.groupby('Email')['Payment ID'].nunique()
                unlimit['email_operation_count'] = unlimit['Email'].map(df_ip_counts_unlimit)
                
                # 2. Несоответствие страны платежа и IP-адреса (исключая NaN)
                unlimit['geo_mismatch'] = (unlimit['IP country'].notna() & unlimit['Card country'].notna() & 
                                      (unlimit['IP country'] != unlimit['Card country']))
                
                # 3. Подсчет количества разных email для одной карты
                unlimit['CARD_BIN'] = unlimit['Card number'].str[:6]
                unlimit['LAST_FOUR'] = unlimit['Card number'].str[-4:]
                card_bin_email_counts_unlimit = unlimit.groupby(['CARD_BIN', 'LAST_FOUR'])['Email'].nunique()
                unlimit['card_used_by_different_emails'] = unlimit.set_index(['CARD_BIN', 'LAST_FOUR']).index.map(card_bin_email_counts_unlimit)
                
                # 4. Подсчет количества разных карт для одного email
                df_email_card_counts_unlimit = unlimit.groupby('Email')['LAST_FOUR'].nunique()
                unlimit['different_cards_per_email'] = unlimit['Email'].map(df_email_card_counts_unlimit)
                
                # 5. Мультиаккаунтинг (один IP на много аккаунтов)
                df_multiacc_unlimit = unlimit.groupby('Customer IP')['Email'].nunique()
                unlimit['multiacc'] = unlimit['Customer IP'].map(df_multiacc_unlimit)
                
                # 6. Количество уникальных BIN на email
                bin_per_email_unlimit = unlimit.groupby("Email")["CARD_BIN"].nunique()
                unlimit["unique_bins_per_email"] = unlimit["Email"].map(bin_per_email_unlimit).fillna(0).astype(int)
                
                # 7. Количество уникальных имен владельцев карт на email
                names_per_email_unlimit = unlimit.groupby("Email")["Card Holder"].nunique()
                unlimit["unique_card_names_per_email"] = unlimit["Email"].map(names_per_email_unlimit).fillna(0).astype(int)
                
                # 8. Количество email на одного владельца карты
                emails_per_card_name_unlimit = unlimit.groupby("Card Holder")["Email"].nunique()
                unlimit["unique_emails_per_card_name"] = unlimit["Card Holder"].map(emails_per_card_name_unlimit).fillna(0).astype(int)
                
                # 9. Выявление неестественно больших сумм
                unlimit['large_payment'] = unlimit['Amount'] > unlimit['Amount'].quantile(0.95)
                
                # 10. Подсчет успешных и неуспешных транзакций
                successful_transactions_unlimit = unlimit[(unlimit['Order type'] == 'Payment') & (unlimit['Status'] == 'Captured')]
                unsuccessful_transactions_unlimit = unlimit[(unlimit['Order type'] == 'Payment') & (unlimit['Status'] == 'Declined')]
                fraud_related_transactions_unlimit = unlimit[unlimit['Status'].isin(['Chargeback'])]
                
                # 11. Учет fraud_related_transactions
                fraud_transactions_count_unlimit = fraud_related_transactions_unlimit.groupby('Email').size()
                unlimit['fraud_transactions_count'] = unlimit['Email'].map(fraud_transactions_count_unlimit).fillna(0)
                
                # 12. Расчет отношения неуспешных транзакций к успешным
                failure_ratio_unlimit = unsuccessful_transactions_unlimit.groupby('Email').size() / successful_transactions_unlimit.groupby('Email').size()
                unlimit['failure_ratio'] = unlimit['Email'].map(failure_ratio_unlimit).fillna(0)
                
                stats_dict_unlimit = {col: calculate_percentiles_and_median(unlimit[col]) for col in [
                    'email_operation_count', 
                    'card_used_by_different_emails', 
                    'different_cards_per_email', 
                    'multiacc', 
                    'unique_bins_per_email',
                    'unique_card_names_per_email', 
                    'unique_emails_per_card_name', 
                    'failure_ratio', 
                    'fraud_transactions_count'
                ]}
                
                stats_df_unlimit = pd.DataFrame(stats_dict_unlimit).T
                
                # Применение порогов
                df_stats_unlimit = unlimit.copy()
                for col in stats_dict_unlimit.keys():
                    threshold_unlimit = stats_df_unlimit.loc[col, '95th_percentile']
                    df_stats_unlimit[f'is_fraud_{col}'] = df_stats_unlimit[col] > threshold_unlimit
                
                # Флаг для geo_mismatch
                df_stats_unlimit['is_fraud_geo_mismatch'] = df_stats_unlimit['geo_mismatch']
                
                # Флаг для больших сумм
                df_stats_unlimit['is_fraud_large_payment'] = df_stats_unlimit['large_payment']
                
                weights_unlimit = {
                    'is_fraud_email_operation_count': 0.1,
                    'is_fraud_different_cards_per_email': 0.25,
                    'is_fraud_card_used_by_different_emails': 0.25,
                    'is_fraud_geo_mismatch': 0.1,
                    'is_fraud_unique_bins_per_email': 0.25,
                    'is_fraud_unique_card_names_per_email': 0.2,
                    'is_fraud_unique_emails_per_card_name': 0.25,
                    'is_fraud_large_payment': 0.1,
                    'is_fraud_failure_ratio': 0.15,
                    'is_fraud_fraud_transactions_count': 1
                }
                
                # Рассчитываем суммарный вес
                total_weight_unlimit = sum(weights_unlimit.values())
                
                # Группировка по email для вычисления суммарных флагов
                df_unique_emails_unlimit = df_stats_unlimit.drop_duplicates(subset='Email')
                df_user_stats_unlimit = df_unique_emails_unlimit.groupby('Email')[list(weights_unlimit.keys())].sum()
                
                # Подсчет итогового fraud_score с нормализацией
                for col, weight in weights_unlimit.items():
                    df_user_stats_unlimit[col] *= weight
                
                df_user_stats_unlimit['fraud_score_unlimit'] = df_user_stats_unlimit[list(weights_unlimit.keys())].sum(axis=1)
                
                # Сортировка по fraud_score
                fraud_users_sorted_unlimit = df_user_stats_unlimit.sort_values(by='fraud_score_unlimit', ascending=False)
                
                fraud_users_sorted_unlimit = fraud_users_sorted_unlimit.reset_index()
                # Выводим результат
                df = fraud_users_sorted_unlimit.copy()
            elif df_name == "df_payabl":
                df = pd.read_csv(path)
                payabl = df.copy()
                # 1. Поиск массовых попыток платежей с одного Email
                df_ip_counts_payabl = payabl.groupby('EMail')['Order No.'].nunique()
                payabl['email_operation_count'] = payabl['EMail'].map(df_ip_counts_payabl)
                
                # 2. Несоответствие страны платежа и IP-адреса (исключая NaN)
                payabl['geo_mismatch'] = (payabl['Bin Country'] != payabl['IP Country'])
                
                # 3. Подсчет количества разных email для одной карты
                payabl['last_four'] = payabl['Credit Card Number'].str[-4:]
                card_bin_email_counts_payabl = payabl.groupby(['Credit Card Bin', 'last_four'])['EMail'].nunique()
                payabl['card_used_by_different_emails'] = payabl.set_index(['Credit Card Bin', 'last_four']).index.map(card_bin_email_counts_payabl)
                
                # 4. Подсчет количества разных карт для одного email
                df_email_card_counts_payabl = payabl.groupby('EMail')['last_four'].nunique()
                payabl['different_cards_per_email'] = payabl['EMail'].map(df_email_card_counts_payabl)
                
                # 5. Мультиаккаунтинг (один IP на много аккаунтов)
                df_multiacc_payabl = payabl.groupby('Customer-IP')['EMail'].nunique()
                payabl['multiacc'] = payabl['Customer-IP'].map(df_multiacc_payabl)
                
                # 6. Количество уникальных BIN на email
                bin_per_email_payabl = payabl.groupby("EMail")["Credit Card Bin"].nunique()
                payabl["unique_bins_per_email"] = payabl["EMail"].map(bin_per_email_payabl).fillna(0).astype(int)
                
                # 7. Количество уникальных имен владельцев карт на email
                names_per_email_payabl = payabl.groupby("EMail")["Credit Cardholder"].nunique()
                payabl["unique_card_names_per_email"] = payabl["EMail"].map(names_per_email_payabl).fillna(0).astype(int)
                
                # 8. Количество email на одного владельца карты
                emails_per_card_name_payabl = payabl.groupby("Credit Cardholder")["EMail"].nunique()
                payabl["unique_emails_per_card_name"] = payabl["Credit Cardholder"].map(emails_per_card_name_payabl).fillna(0).astype(int)
                
                # 9. Выявление неестественно больших сумм
                payabl['large_payment'] = payabl['Amount'] > payabl['Amount'].quantile(0.95)
                
                # 10. Подсчет успешных и неуспешных транзакций
                successful_transactions_payabl = payabl[(payabl['Tx-Type'] == 'Authorisation') & (payabl['Status'] == 'Successful')]
                unsuccessful_transactions_payabl = payabl[(payabl['Tx-Type'] == 'Authorisation') & (payabl['Status'] == 'Failed')]
                fraud_related_transactions_payabl = payabl[payabl['Tx-Type'].isin(['Chargeback'])]
                
                # 11. Учет fraud_related_transactions
                fraud_transactions_count_payabl = fraud_related_transactions_payabl.groupby('EMail').size()
                payabl['fraud_transactions_count'] = payabl['EMail'].map(fraud_transactions_count_payabl).fillna(0)
                
                # 12. Расчет отношения неуспешных транзакций к успешным
                failure_ratio_payabl = unsuccessful_transactions_payabl.groupby('EMail').size() / successful_transactions_payabl.groupby('EMail').size()
                payabl['failure_ratio'] = payabl['EMail'].map(failure_ratio_payabl).fillna(0)
                
                stats_dict_payabl = {col: calculate_percentiles_and_median(payabl[col]) for col in [
                    'email_operation_count', 
                    'card_used_by_different_emails', 
                    'different_cards_per_email', 
                    'multiacc', 
                    'unique_bins_per_email',
                    'unique_card_names_per_email', 
                    'unique_emails_per_card_name', 
                    'failure_ratio', 
                    'fraud_transactions_count'
                ]}
                
                stats_df_payabl = pd.DataFrame(stats_dict_payabl).T
                
                # Применение порогов
                df_stats_payabl = payabl.copy()
                for col in stats_dict_payabl.keys():
                    threshold_payabl = stats_df_payabl.loc[col, '95th_percentile']
                    df_stats_payabl[f'is_fraud_{col}'] = df_stats_payabl[col] > threshold_payabl
                
                # Флаг для geo_mismatch
                df_stats_payabl['is_fraud_geo_mismatch'] = df_stats_payabl['geo_mismatch']
                
                # Флаг для больших сумм
                df_stats_payabl['is_fraud_large_payment'] = df_stats_payabl['large_payment']
                
                weights_payabl = {
                    'is_fraud_email_operation_count': 0.1,
                    'is_fraud_different_cards_per_email': 0.25,
                    'is_fraud_card_used_by_different_emails': 0.25,
                    'is_fraud_geo_mismatch': 0.1,
                    'is_fraud_unique_bins_per_email': 0.25,
                    'is_fraud_unique_card_names_per_email': 0.2,
                    'is_fraud_unique_emails_per_card_name': 0.25,
                    'is_fraud_large_payment': 0.1,
                    'is_fraud_failure_ratio': 0.15,
                    'is_fraud_fraud_transactions_count': 1
                }
                
                # Рассчитываем суммарный вес
                total_weight_payabl = sum(weights_payabl.values())
                
                # Группировка по email для вычисления суммарных флагов
                df_unique_emails_payabl = df_stats_payabl.drop_duplicates(subset='EMail')
                df_user_stats_payabl = df_unique_emails_payabl.groupby('EMail')[list(weights_payabl.keys())].sum()
                
                # Подсчет итогового fraud_score с нормализацией
                for col, weight in weights_payabl.items():
                    df_user_stats_payabl[col] *= weight
                
                df_user_stats_payabl['fraud_score_payabl'] = df_user_stats_payabl[list(weights_payabl.keys())].sum(axis=1)
                
                # Сортировка по fraud_score
                fraud_users_sorted_payabl = df_user_stats_payabl.sort_values(by='fraud_score_payabl', ascending=False)

                fraud_users_sorted_payabl = fraud_users_sorted_payabl.reset_index()
                
                df = fraud_users_sorted_payabl.copy()

            elif df_name == "df_centrobill":
                df = pd.read_csv(path, sep=";")
                
                centrobill = df.copy()
                
                centrobill = centrobill[centrobill['Payment method'].isin(['visa', 'mastercard'])].copy()

                centrobill = centrobill[centrobill['Test'] == 'no'].copy()
                
                # 1. Поиск массовых попыток платежей с одного Email
                df_ip_counts_centrobill = centrobill.groupby('E-mail')['Transaction ID'].nunique()
                centrobill['email_operation_count'] = centrobill['E-mail'].map(df_ip_counts_centrobill)
                
                # 2. Подсчет количества разных email для одной карты
                card_bin_email_counts_centrobill = centrobill.groupby(['Bin', 'Last four'])['E-mail'].nunique()
                centrobill['card_used_by_different_emails'] = centrobill.set_index(['Bin', 'Last four']).index.map(card_bin_email_counts_centrobill)
                
                # 3. Подсчет количества разных карт для одного email
                df_email_card_counts_centrobill = centrobill.groupby('E-mail')['Last four'].nunique()
                centrobill['different_cards_per_email'] = centrobill['E-mail'].map(df_email_card_counts_centrobill)
                
                # 4. Количество уникальных BIN на email
                centrobill["Bin"] = centrobill["Bin"].astype(str)
                centrobill['Bin'] = centrobill['Bin'].str[:6]
                bin_per_email_centrobill = centrobill.groupby("E-mail")["Bin"].nunique()
                centrobill["unique_bins_per_email"] = centrobill["E-mail"].map(bin_per_email_centrobill).fillna(0).astype(int)
                
                # 5. Количество уникальных имен владельцев карт на email
                names_per_email_centrobill = centrobill.groupby("E-mail")["Customer name"].nunique()
                centrobill["unique_card_names_per_email"] = centrobill["E-mail"].map(names_per_email_centrobill).fillna(0).astype(int)
                
                # 6. Количество email на одного владельца карты
                emails_per_card_name_centrobill = centrobill.groupby("Customer name")["E-mail"].nunique()
                centrobill["unique_emails_per_card_name"] = centrobill["Customer name"].map(emails_per_card_name_centrobill).fillna(0).astype(int)
                
                # 7. Выявление неестественно больших сумм
                centrobill['large_payment'] = centrobill['USD Cost'] > centrobill['USD Cost'].quantile(0.95)
                
                # 8. Подсчет успешных и неуспешных транзакций
                successful_transactions_centrobill = centrobill[(centrobill['Type'].isin(['Initial', 'Non-Recurring', 'Recurring'])) & (centrobill['Status'] == 'success')]
                unsuccessful_transactions_centrobill = centrobill[(centrobill['Type'].isin(['Initial', 'Non-Recurring', 'Recurring'])) & (centrobill['Status'] == 'fail')]
                fraud_related_transactions_centrobill = centrobill[centrobill['Type'].isin(['Chargeback'])]
                
                # 9. Учет fraud_related_transactions
                fraud_transactions_count_centrobill = fraud_related_transactions_centrobill.groupby('E-mail').size()
                centrobill['fraud_transactions_count'] = centrobill['E-mail'].map(fraud_transactions_count_centrobill).fillna(0)
                
                # 10. Расчет отношения неуспешных транзакций к успешным
                failure_ratio_centrobill = unsuccessful_transactions_centrobill.groupby('E-mail').size() / successful_transactions_centrobill.groupby('E-mail').size()
                centrobill['failure_ratio'] = centrobill['E-mail'].map(failure_ratio_centrobill).fillna(0)
                
                stats_dict_centrobill = {col: calculate_percentiles_and_median(centrobill[col]) for col in [
                    'email_operation_count', 
                    'card_used_by_different_emails', 
                    'different_cards_per_email', 
                    'unique_bins_per_email',
                    'unique_card_names_per_email', 
                    'unique_emails_per_card_name', 
                    'failure_ratio', 
                    'fraud_transactions_count'
                ]}
                
                stats_df_centrobill = pd.DataFrame(stats_dict_centrobill).T
                
                # Применение порогов
                df_stats_centrobill = centrobill.copy()
                for col in stats_dict_centrobill.keys():
                    threshold_centrobill = stats_df_centrobill.loc[col, '95th_percentile']
                    df_stats_centrobill[f'is_fraud_{col}'] = df_stats_centrobill[col] > threshold_centrobill
                
                # Флаг для больших сумм
                df_stats_centrobill['is_fraud_large_payment'] = df_stats_centrobill['large_payment']
                
                weights_centrobill = {
                    'is_fraud_email_operation_count': 0.1,
                    'is_fraud_different_cards_per_email': 0.25,
                    'is_fraud_card_used_by_different_emails': 0.25,
                    'is_fraud_unique_bins_per_email': 0.25,
                    'is_fraud_unique_card_names_per_email': 0.2,
                    'is_fraud_unique_emails_per_card_name': 0.25,
                    'is_fraud_large_payment': 0.1,
                    'is_fraud_failure_ratio': 0.15,
                    'is_fraud_fraud_transactions_count': 0.5
                }
                
                # Рассчитываем суммарный вес
                total_weight_centrobill = sum(weights_centrobill.values())
                
                # Группировка по email для вычисления суммарных флагов
                df_unique_emails_centrobill = df_stats_centrobill.drop_duplicates(subset='E-mail')
                
                df_user_stats_centrobill = df_unique_emails_centrobill.groupby('E-mail')[list(weights_centrobill.keys())].sum()
                
                # Подсчет итогового fraud_score с нормализацией
                for col, weight in weights_centrobill.items():
                    df_user_stats_centrobill[col] *= weight
                
                df_user_stats_centrobill['fraud_score_centrobill'] = df_user_stats_centrobill[list(weights_centrobill.keys())].sum(axis=1) / total_weight_centrobill
                
                # Сортировка по fraud_score
                fraud_users_sorted_centrobill = df_user_stats_centrobill.sort_values(by='fraud_score_centrobill', ascending=False)
                
                fraud_users_sorted_centrobill = fraud_users_sorted_centrobill.reset_index()
                
                df = fraud_users_sorted_centrobill.copy()
            else:
                # Пример добавки source метки для остальных (можно убрать)
                df['source'] = df_name
            # Сохраняем результат
            result_path = f"temp/{user_id}/result_{df_name}_{filename}"
            df.to_csv(result_path, index=False)
            await update.message.reply_document(document=open(result_path, "rb"))
        
        await update.message.reply_text("✅ Все файлы обработаны и отправлены.")
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка: {e}")
    finally:
        user_sessions.pop(user_id, None)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    doc = update.message.document

    if doc.mime_type != 'text/csv':
        await update.message.reply_text("⚠️ Принимаю только CSV-файлы.")
        return

    os.makedirs(f"temp/{user_id}", exist_ok=True)
    file_path = f"temp/{user_id}/{doc.file_name}"
    file = await doc.get_file()
    await file.download_to_drive(file_path)

    if user_id in user_sessions:
        user_sessions[user_id].append(file_path)
        await update.message.reply_text(f"📎 Файл '{doc.file_name}' получен.")
    else:
        await update.message.reply_text("⚠️ Сначала введи /batch перед загрузкой.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        await update.message.reply_text("⛔ Бот завершает работу.")
        await context.application.stop()
        sys.exit(0)
    else:
        await update.message.reply_text("🚫 У тебя нет прав останавливать бота.")

if __name__ == '__main__':
    import asyncio

    async def main():
        app = ApplicationBuilder().token(TOKEN).build()

        # Удаляем старый Webhook, если остался
        await app.bot.delete_webhook(drop_pending_updates=True)

        # Добавляем обработчики
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("batch", batch))
        app.add_handler(CommandHandler("done", done))
        app.add_handler(CommandHandler("stop", stop))
        app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

        print("🚀 Бот стартует через polling...")
        await app.run_polling()

    asyncio.run(main())
