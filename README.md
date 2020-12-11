# Frege RabbitMQ Consumer test application

## Local run
To run this app locally you need a Python3 and `pika` lib.
To install `pika` run `pip3 install pika`

Then just type `RABBITMQ_HOST="<rabbit host ip or hostname>" QUEUE="<source queue name>" python3 main.py` (may vary on Windows - consult documentation how to pass an environment variables to a program)

## Docker application
Docker image of this app is available as **jagiellonian/frege-rabbitmq-consumer**

To run just type `docker run -it -e RABBITMQ_HOST="<rabbit host ip or hostname>" -e QUEUE="<destination queue name>" jagiellonian/frege-rabbitmq-consumer`
