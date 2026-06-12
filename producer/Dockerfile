FROM python:3.13

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY vehicle_positions_producer.py ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "vehicle_positions_producer.py"]
