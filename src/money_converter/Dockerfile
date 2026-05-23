FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY money_converter/money_converter.py .

ENTRYPOINT ["python3", "money_converter.py"]
