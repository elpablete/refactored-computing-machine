import datetime as dt
import logging
import pathlib

import pydantic_settings
import redis

logger = logging.getLogger("get_result")


class Settings(pydantic_settings.BaseSettings):
    REDIS_DB: int = 0
    REDIS_HOST: str = "redis-service"
    REDIS_PASSWORD: str
    REDIS_PORT: int = 6379
    # REDIS_TLS: "true"
    STREAM_NAME: str


settings = Settings()


DATA_FOLDER = pathlib.Path("data")
BATCH_SIZE = 100


def main():
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    redis_client.ping()

    result_snapshot_time = dt.datetime.now().strftime("%Y%m%d-%H%M%S")

    with open(DATA_FOLDER / f"{result_snapshot_time}_db.txt", "w") as d:
        logger.debug("Get all keys from db")
        for i, k in enumerate(redis_client.scan_iter(count=BATCH_SIZE)):
            if i % BATCH_SIZE == 0:
                logger.debug(f"Gotten {i} keys from db")
            d.write(f"{k}\n")

    with open(DATA_FOLDER / f"{result_snapshot_time}_db.txt", "r") as s, open(
        DATA_FOLDER / f"{result_snapshot_time}_result.txt", "w"
    ) as d:
        logger.debug(f"Get all ids in db from stream: {settings.STREAM_NAME}")
        for i, line in enumerate(s):
            if i % BATCH_SIZE == 0:
                logger.debug(f"Gotten {i} keys from db")
            tx_id = line.strip()

            try:
                count = redis_client.get(tx_id)
            except Exception as e:
                if "WRONGTYPE" in str(e):
                    logger.debug(f"Key {tx_id} is not a string")
                    continue

            d.write(f"{tx_id},{count}\n")


if __name__ == "__main__":
    main()
