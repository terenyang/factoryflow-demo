FROM python:3.12-slim

WORKDIR /app

COPY services/validator-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY services/validator-service ./service

ENV PYTHONPATH=/app/shared

WORKDIR /app/service

CMD ["python", "main.py"]
