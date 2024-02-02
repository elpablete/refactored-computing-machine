import logging
import random
import sys
import time
import uuid

import pydantic
import pydantic_settings
import redis
import redsumer


class Settings(pydantic_settings.BaseSettings):
    REDIS_DB: int = 0
    REDIS_HOST: str = "redis-service"
    REDIS_PASSWORD: str
    REDIS_PORT: int = 6379
    # REDIS_TLS: "true"
    STREAM_NAME: str
    CONSUMER_GROUP_NAME: str
    CONSUMER_NAME: str
    CONSUMER_MESSAGE_BATCH_SIZE: int
    PROCESS_TIME_MEAN: float
    PROCESS_TIME_VARIANCE: float
    PROCESS_FAILURE_CHANCE: float
    CONSUMER_PER_CYCLE_FAILUIRE_CHANCE: float


settings = Settings()
settings.CONSUMER_NAME = f"{settings.CONSUMER_NAME}-{uuid.uuid4()}"

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(settings.CONSUMER_NAME)


class Message(pydantic.BaseModel):
    tx_id: uuid.UUID


def process_message(message: Message, persistance_dependency: redis.Redis):
    logger.info(f"Processing message: {message}")
    process_time_in_seconds = random.lognormvariate(
        settings.PROCESS_TIME_MEAN, settings.PROCESS_TIME_VARIANCE
    )
    time.sleep(process_time_in_seconds)
    if random.random() < settings.PROCESS_FAILURE_CHANCE:
        raise Exception("Processing failed")
    else:
        persistance_dependency.incr(str(message.tx_id))


def main() -> None:
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    redis_client.ping()

    try:
        redis_client.xgroup_create(
            settings.STREAM_NAME, settings.CONSUMER_GROUP_NAME, id="0-0"
        )
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group {settings.CONSUMER_GROUP_NAME} already exists")
        else:
            raise e

    logger.debug(
        f"Consuming from stream: {settings.STREAM_NAME} as {settings.CONSUMER_NAME} in group {settings.CONSUMER_GROUP_NAME}"
    )
    consumer = redsumer.RedSumer(
        client=redis_client,
        stream=settings.STREAM_NAME,
        group=settings.CONSUMER_GROUP_NAME,
        name=settings.CONSUMER_NAME,
        batch_size=10,
        claim_batch_size=10,
        # block_milliseconds=5 * 1_000,
        min_milliseconds_to_claim_idle=5 * 1_000,
    )
    logger.info(f"consumer: {consumer}")

    for batch in consumer.consume():
        ##################################################################################
        ## system failure section
        #################################################################################
        if random.random() < settings.CONSUMER_PER_CYCLE_FAILUIRE_CHANCE:
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
