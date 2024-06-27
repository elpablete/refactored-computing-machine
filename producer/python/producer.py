import logging
import pathlib
import uuid

import pydantic
import pydantic_settings
import redis
import rtoml


class ProducerSettings(pydantic_settings.BaseSettings):
    STREAM_NAME: str


class RedisSettings(pydantic_settings.BaseSettings):
    model_config = pydantic_settings.SettingsConfigDict(env_prefix="REDIS_")
    DB: int = 0
    HOST: str
    PASSWORD: pydantic.SecretStr
    PORT: int


class Settings(pydantic_settings.BaseSettings):
    redis: RedisSettings = RedisSettings()
    producer: ProducerSettings = ProducerSettings(
        **rtoml.load(pathlib.Path("test.toml")).get("producer", {})
    )


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
        settings.producer.STREAM_NAME, message.model_dump(mode="json")
    )
    persistance_dependency.set(str(message.tx_id), 0)
    return message_id


def main(count: int):
    redis_client = redis.Redis(
        host=settings.redis.HOST,
        port=settings.redis.PORT,
        password=settings.redis.PASSWORD,
        decode_responses=True,
    )
    redis_client.ping()

    logger.debug(f"Publish to stream: {settings.producer.STREAM_NAME}")
    for i in range(count):
        #################################################################################
        logger.info("Publish message")
        message = produce_message()
        _ = publish_message(message, redis_client)


if __name__ == "__main__":
    main(1_000)
