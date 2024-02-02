import enum
import logging
from typing import Generator, TypeVar

import redis
from typing_extensions import Self

logger = logging.getLogger(__name__)
T = TypeVar("T")


class RedisSpecialId(str, enum.Enum):
    NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR = ">"
    GREATEST_ID_INSIDE_THE_STREAM = "$"
    FIRST_ID_INSIDE_THE_STREAM = "0-0"


class RedSumer:
    def __init__(
        self,
        client: redis.Redis,
        stream: str,
        group: str,
        name: str,
        *,
        start_group_since_id: str = RedisSpecialId.FIRST_ID_INSIDE_THE_STREAM,
        block_milliseconds: int | None = None,
        batch_size: int | None = None,
        pending_batch_size: int | None = None,
        claim_batch_size: int | None = None,
        min_milliseconds_to_claim_idle: int = 1_000,
    ) -> None:
        self.client = client
        self.stream = stream
        self.group = group
        self.name = name
        self.latest_pending_msg_id = RedisSpecialId.FIRST_ID_INSIDE_THE_STREAM
        self.block = block_milliseconds
        self.batch_size = batch_size
        self.pending_batch_size = pending_batch_size or self.batch_size
        self.claim_batch_size = claim_batch_size or self.pending_batch_size
        self.min_milliseconds_to_claim_idle = min_milliseconds_to_claim_idle

        # check connection
        self.client.ping()

        # create csonsumer group if not exists
        try:
            logger.info("Create consumer group if not exists")
            self.client.xgroup_create(
                self.stream,
                groupname=self.group,
                id=0,  # start_group_since_id,
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{self.group}' already exists")
            else:
                raise e
        return None

    def __iter__(self) -> Self:
        return self

    def __str__(self) -> str:
        return f"Redis Consumer from host: {self.client}, stream '{self.stream}', in consumer group '{self.group}' as '{self.name}'"

    def ack(self: Self, message_id: str) -> None:
        self.client.xack(self.stream, self.group, message_id)

    def still_mine(self: Self, message_id: str) -> bool:
        xpending_range_response = self.client.xpending_range(
            name=self.stream,
            groupname=self.group,
            min=message_id,
            max=message_id,
            count=1,
            consumername=self.name,
        )
        if len(xpending_range_response) > 0:
            return True
        else:
            return False

    def consume(self) -> Generator[list[tuple[str, T]], None, None]:
        return next(self)

    def __next__(self: Self) -> list[tuple[str, T]]:
        while True:
            #################################################################################
            ## new messages section
            #################################################################################
            while True:
                logger.info("Polling for new messages")
                xreadgroup_response = self.client.xreadgroup(
                    groupname=self.group,
                    consumername=self.name,
                    streams={
                        self.stream: RedisSpecialId.NEVER_DELIVERED_TO_OTHER_CONSUMERS_SO_FAR
                    },
                    count=self.batch_size,
                    block=self.block,
                )
                if len(xreadgroup_response) > 0:
                    my_new_work: list[tuple[str, T]] = xreadgroup_response[0][1]
                else:
                    my_new_work = []

                if len(my_new_work) > 0:
                    logger.info(f"Yielding new messages: {len(my_new_work)}")
                    yield my_new_work
                else:
                    logger.info("No new messages")
                    break  # break out of the new messages loop

            #################################################################################
            ## pending messages section
            #################################################################################
            logger.info("Polling for pending messages")

            xreadgroup_response = self.client.xreadgroup(
                groupname=self.group,
                consumername=self.name,
                streams={self.stream: self.latest_pending_msg_id},
                count=self.pending_batch_size,
                block=self.block,
            )

            if len(xreadgroup_response) > 0:
                my_pending_work: list[tuple[str, T]] = xreadgroup_response[0][1]
            else:
                my_pending_work = []

            if len(my_pending_work) > 0:
                last_msg_in_pending_batch_id = my_pending_work[-1][0]
                self.latest_pending_msg_id = last_msg_in_pending_batch_id
                logger.info(f"Yielding pending messages: {len(my_pending_work)}")
                yield my_pending_work
            else:
                # claim messages that are pending for than min_idle_milliseconds in this CONSUMER_GROUP
                logger.info("No pending messages")
                self.latest_pending_msg_id = RedisSpecialId.FIRST_ID_INSIDE_THE_STREAM
                logger.info("Claiming pending messages")
                autoclaim_response = self.client.xautoclaim(
                    name=self.stream,
                    groupname=self.group,
                    consumername=self.name,
                    min_idle_time=self.min_milliseconds_to_claim_idle,
                    start_id=RedisSpecialId.FIRST_ID_INSIDE_THE_STREAM,
                    count=self.claim_batch_size,
                    justid=True,
                )
                if len(autoclaim_response) > 0:
                    total_claimed = len(autoclaim_response)
                    logger.info(f"Claimed {total_claimed} messages")
                else:
                    logger.info(
                        f"No idle messages in consumer group '{self.group}' of stream '{self.stream}'"
                    )
                continue
