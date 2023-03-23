from datetime import timedelta

import httpx

from domain.entity import ServiceConnectorEntity
from kombu import Connection, Exchange, Queue, serialization
from loguru import logger


class RabbitMQFileReceiver:
    def __init__(self, broker_url: str, exchange: str) -> None:
        self.broker_url: str = broker_url
        self.exchange: str = exchange

        self.services_connector: list[ServiceConnectorEntity] = []

    def add_service(self, service_connector_entity: ServiceConnectorEntity):
        self.services_connector.append(service_connector_entity)

    def run(self):
        conn = Connection(self.broker_url)
        exchange = Exchange(self.exchange, type='direct')

        serialization.enable_insecure_serializers()

        with conn.channel() as channel:
            consumers = []

            for service_connector in self.services_connector:
                for queue_name in service_connector.queues:
                    queue = Queue(queue_name, exchange, routing_key=queue_name)
                    queue.maybe_bind(conn)
                    queue.declare()

                    consumer = conn.Consumer(queue, callbacks=[self.process_message(service_connector)])
                    consumer.consume()
                    consumers.append(consumer)

            # Начинаем прослушивание
            while True:
                try:
                    conn.drain_events()
                except KeyboardInterrupt:
                    break

            # Останавливаем Consumer для каждой очереди
            for consumer in consumers:
                consumer.cancel()

    def process_message(self, service_connector):
        def inner_process_message(body, message):
            logger.info(
                f"Received message from {message.delivery_info['routing_key']} for service {service_connector}")

            try:
                with httpx.Client() as client:
                    timeout = timedelta(seconds=10)

                    files = {"rinex": body['chunk']}

                    response = client.post(service_connector.url, files=files, timeout=timeout.total_seconds())
                response.raise_for_status()
                logger.info(f"File sent to {service_connector.url} with response status {response.status_code}")
                message.ack()

            except Exception as e:
                logger.error(f"Failed to send file to {service_connector.url}: {e}")

        return inner_process_message
