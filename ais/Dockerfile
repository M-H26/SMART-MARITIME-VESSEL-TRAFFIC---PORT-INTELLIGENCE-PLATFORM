FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ais_stream_producer.py .

CMD ["python", "-u", "ais_stream_producer.py"]
