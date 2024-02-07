import logging
import uuid

import pydantic
import pydantic_settings
import redis


class Settings(pydantic_settings.BaseSettings):
    REDIS_DB: int = 0
    REDIS_HOST: str
    REDIS_PASSWORD: str
    REDIS_PORT: int
    # REDIS_TLS: "true"
    STREAM_NAME: str


settings = Settings()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("PRODUCER")


class Message(pydantic.BaseModel):
    tx_id: uuid.UUID


def produce_message():
    return Message(tx_id=uuid.uuid4())


def publish_message(message: Message, persistance_dependency: redis.Redis):
    logger.info(f"Publish message: {message}")
    message_id = persistance_dependency.xadd(
        settings.STREAM_NAME, message.model_dump(mode="json")
    )
    persistance_dependency.set(str(message.tx_id), 0)
    return message_id


def main(count: int):
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    redis_client.ping()

    logger.debug(f"Publish to stream: {settings.STREAM_NAME}")
    for i in range(count):
        #################################################################################
        logger.info("Publish message")
        message = produce_message()
        _ = publish_message(message, redis_client)


if __name__ == "__main__":
    main(1_000)
