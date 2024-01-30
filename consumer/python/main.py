import enum
import logging
import random
import sys
import time
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
    CONSUMER_NAME: str
    CONSUMER_GROUP_NAME: str
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


class RedisSpecialId(str, enum.Enum):
    NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR = ">"
    GREATEST_ID_INSIDE_THE_STREAM = "$"
    FIRST_ID_INSIDE_THE_STREAM = "0-0"


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


def process_messages(messages: list[Message], persistance_dependency: redis.Redis):
    for message in messages:
        message_id = message[0]
        message_content = Message.model_validate(message[1])
        try:
            process_message(message_content, persistance_dependency)
        except Exception:
            logger.exception(f"Error processing message: {message}")
            continue
        persistance_dependency.xack(
            settings.STREAM_NAME, settings.CONSUMER_GROUP_NAME, message_id
        )


def main():
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    groups_info = redis_client.xinfo_groups(
        settings.STREAM_NAME,
    )
    logger.info(f"Groups info: {groups_info}")

    try:
        redis_client.xgroup_create(settings.STREAM_NAME, settings.CONSUMER_GROUP_NAME)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group {settings.CONSUMER_GROUP_NAME} already exists")
        else:
            raise e

    logger.debug(
        f"Consuming from stream: {settings.STREAM_NAME} as {settings.CONSUMER_NAME} in group {settings.CONSUMER_GROUP_NAME}"
    )
    max_id_seen = RedisSpecialId.FIRST_ID_INSIDE_THE_STREAM

    while True:
        #################################################################################
        logger.info(
            f"Polling for new messages 'XREADGROUP {RedisSpecialId.NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR}'"
        )
        xreadgroup_response = redis_client.xreadgroup(
            settings.CONSUMER_GROUP_NAME,
            settings.CONSUMER_NAME,
            {
                settings.STREAM_NAME: RedisSpecialId.NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR
            },
            count=settings.CONSUMER_MESSAGE_BATCH_SIZE,
        )
        if len(xreadgroup_response) > 0:
            my_new_work_list = xreadgroup_response[0][1]
            logger.info(f"Processing new messages: {len(my_new_work_list)}")
            process_messages(my_new_work_list, redis_client)
        else:
            logger.info("No new messages")

        #################################################################################
        logger.info(f"Polling for pending messages 'XREADGROUP {max_id_seen}'")

        xreadgroup_response = redis_client.xreadgroup(
            settings.CONSUMER_GROUP_NAME,
            settings.CONSUMER_NAME,
            {settings.STREAM_NAME: max_id_seen},
            count=settings.CONSUMER_MESSAGE_BATCH_SIZE,
        )
        if len(xreadgroup_response) > 0:
            my_pending_work_list = xreadgroup_response[0][1]
            logger.info(f"Processing pending messages: {len(my_pending_work_list)}")
            process_messages(my_pending_work_list, redis_client)
        else:
            logger.info("No pending messages")

        ##################################################################################
        if random.random() < settings.CONSUMER_PER_CYCLE_FAILUIRE_CHANCE:
            logger.error("Consumer failed")
            sys.exit()


if __name__ == "__main__":
    main()
