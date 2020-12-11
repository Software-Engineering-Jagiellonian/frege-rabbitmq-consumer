import os
import pika
import sys


def callback(ch, method, properties, body):
    ch.stop_consuming()
    print(f" [x] Received:")
    print(body.decode('utf-8'))
    input("Press ENTER to mark this message as processed (acknowledge)")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def app(rabbitmq_host, rabbitmq_port, queue):
    while True:
        try:
            print(f"Connecting to RabbitMQ ({rabbitmq_host}:{rabbitmq_port})...")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))
            channel = connection.channel()
            print("Connected")

            channel.queue_declare(queue=queue, durable=True)

            while True:
                channel.basic_consume(queue=queue,
                                      auto_ack=False,
                                      on_message_callback=callback)

                print(' [*] Waiting for a message')
                channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as exception:
            print(f"AMQP Connection Error: {exception}")
        except KeyboardInterrupt:
            print(" Exiting...")
            try:
                connection.close()
            except NameError:
                pass
            sys.exit(0)


if __name__ == '__main__':
    try:
        rabbitmq_host = os.environ['RABBITMQ_HOST']
    except KeyError:
        print("RabbitMQ host must be provided as RABBITMQ_HOST environment var!")
        sys.exit(1)

    try:
        rabbitmq_port = int(os.environ.get('RABBITMQ_PORT', '5672'))
    except ValueError:
        print("RABBITMQ_PORT must be an integer")
        sys.exit(2)

    try:
        queue = os.environ['QUEUE']
    except KeyError:
        print("Source queue must be provided as QUEUE environment var!")
        sys.exit(3)

    app(rabbitmq_host, rabbitmq_port, queue)
