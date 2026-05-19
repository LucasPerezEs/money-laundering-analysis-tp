FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app
COPY common ./common
COPY filters ./filters

ENV PYTHONPATH=/app:/app/common

ENTRYPOINT ["python3", "-m", "filters.main"]