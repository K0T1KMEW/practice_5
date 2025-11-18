FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /var/log/screenshot-service

COPY . .

CMD ["python", "main.py"]