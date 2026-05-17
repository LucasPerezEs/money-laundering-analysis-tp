FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY data_reducer .

ENTRYPOINT ["python3", "data_reducer.py"]