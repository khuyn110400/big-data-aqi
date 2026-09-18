FROM python:3.12-slim

WORKDIR /app

COPY serving/requirements.txt /app/serving/requirements.txt
RUN pip install --no-cache-dir -r /app/serving/requirements.txt

COPY serving /app/serving
COPY collector/config/cities.json /app/collector/config/cities.json

EXPOSE 8000

CMD ["uvicorn", "serving.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
