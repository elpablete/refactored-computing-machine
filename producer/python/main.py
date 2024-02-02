import logging
import uuid

import pydantic
import pydantic_settings
import redis


class Settings(pydantic_settings.BaseSettings):
    REDIS_DB: int = 0
    REDIS_HOST: str = "redis-service"
    REDIS_PASSWORD: str
    REDIS_PORT: int = 6379
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
    return message_id


def main():
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    redis_client.ping()

    logger.debug(f"Publish to stream: {settings.STREAM_NAME}")
    with open("db.txt", "w") as f:
        while True:
            #################################################################################
            logger.info("Publish message")
            message = produce_message()
            msg_id = publish_message(message, redis_client)
            f.write(f"{msg_id},{message.tx_id}\n")
            f.flush()


if __name__ == "__main__":
    main()
