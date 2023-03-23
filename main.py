from RabbitMQFileReceiver import RabbitMQFileReceiver
from domain.entity import ServiceConnectorEntity

if __name__ == '__main__':
    rabbitmq_file_receiver: RabbitMQFileReceiver = RabbitMQFileReceiver(
        broker_url="amqp://guest:guest@localhost:5672//",
        exchange="fastapi_magic_large_files"
    )

    rabbitmq_file_receiver.add_service(
        service_connector_entity=ServiceConnectorEntity(
            url="http://127.0.0.1:12000/rinex_to_csv/upload_rinex",
            queues=["file_chunks_queue"]
        )
    )

    rabbitmq_file_receiver.run()
