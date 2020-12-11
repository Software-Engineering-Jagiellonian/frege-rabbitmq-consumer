FROM python:3.8.6-slim-buster

WORKDIR /app

COPY main.py .

RUN pip3 install pika

CMD [ "python3", "main.py" ]
