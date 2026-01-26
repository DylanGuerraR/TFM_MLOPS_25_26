FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# El entrypoint se definirá en el docker-compose o se ejecutará manualmente
CMD ["python", "train_xgboost.py"]
