# Используем минимальный образ Python
FROM python:3.11-slim

# Обновляем пакеты и ставим зависимости для Pandas и distutils
RUN apt-get update && apt-get install -y python3-distutils gcc && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем зависимости Python
RUN pip install --upgrade pip && pip install -r requirements.txt

# Копируем все остальные файлы (включая telegram_bot.py)
COPY . .

# Запускаем бота
CMD ["python", "telegram_bot_test.py"]
