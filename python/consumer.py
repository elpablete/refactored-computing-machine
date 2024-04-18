import logging
import random
import sys
import time
import uuid

import pydantic
import pydantic_settings
import redis
import stream_consumer


class ConsumerSettings(pydantic_settings.BaseSettings):
    STREAM_NAME: str
    CONSUMER_GROUP_NAME: str
    CONSUMER_NAME: str
    CONSUMER_MESSAGE_BATCH_SIZE: int
    CLAIM_BATCH_SIZE: int | None = None
    MIN_MILLISECONDS_TO_CLAIM_IDLE: int | None = None
    BLOCK_MILLISECONDS: int | None = None
    PENDING_BATCH_SIZE: int | None = None


class TestSettings(pydantic_settings.BaseSettings):
    PROCESS_TIME_MEAN: float
    PROCESS_TIME_VARIANCE: float
    PROCESS_FAILURE_CHANCE: float
    CONSUMER_PER_CYCLE_FAILUIRE_CHANCE: float


class RedisSettings(pydantic_settings.BaseSettings):
    model_config = pydantic_settings.SettingsConfigDict(env_prefix="REDIS_")
    DB: int = 0
    HOST: str
    PASSWORD: pydantic.SecretStr
    PORT: int


class Settings(pydantic_settings.BaseSettings):
    redis: RedisSettings = RedisSettings()
    consumer: ConsumerSettings = ConsumerSettings(
        # **rtoml.load(pathlib.Path("test.toml")).get("consumer", {})
    )
    test: TestSettings = TestSettings(
        # **rtoml.load(pathlib.Path("test.toml")).get("test", {})
    )


settings = Settings()
settings.consumer.CONSUMER_NAME = (
    f"{settings.consumer.CONSUMER_NAME}-{uuid.uuid4().hex[:8]}"
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(settings.consumer.CONSUMER_NAME)


class Message(pydantic.BaseModel):
    tx_id: uuid.UUID


def process_message(message: Message, persistance_dependency: redis.Redis):
    logger.info(f"Processing message: {message}")
    process_time_in_seconds = random.lognormvariate(
        settings.test.PROCESS_TIME_MEAN, settings.test.PROCESS_TIME_VARIANCE
    )
    time.sleep(process_time_in_seconds)
    if random.random() < settings.test.PROCESS_FAILURE_CHANCE:
        raise Exception("Processing failed")
    else:
        persistance_dependency.incr(str(message.tx_id))


def main() -> None:
    redis_client = redis.Redis(
        host=settings.redis.HOST,
        port=settings.redis.PORT,
        password=settings.redis.PASSWORD.get_secret_value(),
        decode_responses=True,
    )
    redis_client.ping()

    try:
        redis_client.xgroup_create(
            settings.consumer.STREAM_NAME,
            settings.consumer.CONSUMER_GROUP_NAME,
            id="0-0",
        )
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(
                f"Consumer group {settings.consumer.CONSUMER_GROUP_NAME} already exists"
            )
        else:
            raise e

    logger.debug(
        f"Consuming from stream: {settings.consumer.STREAM_NAME}"
        f" as {settings.consumer.CONSUMER_NAME}"
        f" in group {settings.consumer.CONSUMER_GROUP_NAME}"
    )
    consumer = stream_consumer.Consumer(
        client=redis_client,
        stream=settings.consumer.STREAM_NAME,
        group=settings.consumer.CONSUMER_GROUP_NAME,
        name=settings.consumer.CONSUMER_NAME,
        batch_size=settings.consumer.CONSUMER_MESSAGE_BATCH_SIZE,
        claim_batch_size=settings.consumer.CLAIM_BATCH_SIZE,
        pending_batch_size=settings.consumer.PENDING_BATCH_SIZE,
        block_milliseconds=settings.consumer.BLOCK_MILLISECONDS,
        min_milliseconds_to_claim_idle=settings.consumer.MIN_MILLISECONDS_TO_CLAIM_IDLE,
        message_class=Message,
    )
    logger.info(f"consumer: {consumer}")

    for batch in consumer.consume():
        ##################################################################################
        ## system failure section
        #################################################################################
        if random.random() < settings.test.CONSUMER_PER_CYCLE_FAILUIRE_CHANCE:
            logger.error("Consumer failed")
            sys.exit()

        #################################################################################
        ## Proccess batch
        #################################################################################
        for msg_id, m in batch:
            message = Message.model_validate(m)
            if consumer.still_mine(msg_id):
                try:
                    process_message(message, redis_client)
                except Exception:
                    logger.exception(
                        f"Error processing message: {message}, continue with next"
                    )
                    continue
                consumer.ack(msg_id)
            else:
                logger.warn(
                    f"Ownership failed, can't process message with id '{msg_id}'"
                )


if __name__ == "__main__":
    main()
