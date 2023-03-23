from pydantic import BaseModel


class ServiceConnectorEntity(BaseModel):
    url: str
    queues: list[str]
