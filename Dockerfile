# Используйте официальный образ Python как базовый
FROM python:3.12

# Установите рабочий каталог в контейнере
WORKDIR /app

# Скопируйте файлы зависимостей в контейнер
COPY requirements.txt .

# Установите зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Скопируйте исходный код проекта в контейнер
COPY . .

# Запускайте manager/main.py по умолчанию
CMD ["python", "./manager/main.py"]