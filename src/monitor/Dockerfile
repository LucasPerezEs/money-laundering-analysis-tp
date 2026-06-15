FROM python:3.12-alpine

RUN pip install pika docker

WORKDIR /app
COPY . .

ENTRYPOINT ["python3", "monitor.py"]