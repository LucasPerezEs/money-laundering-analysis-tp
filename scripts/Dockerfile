FROM python:3.9-slim
RUN pip install docker
WORKDIR /app
COPY chaos_monkey.py /app/
CMD ["python3", "-u", "chaos_monkey.py"]
