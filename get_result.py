import logging

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
logger = logging.getLogger("KEY_GETTER")


def main():
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    redis_client.ping()

    with open("keys.txt", "w") as f:
        for k in redis_client.xrange(settings.STREAM_NAME):
            f.write(f"{k[0]},{k[1]['tx_id']}\n")

    with open("keys.txt", "r") as s, open("db.txt", "w") as d:
        for line in s:
            # key = line.split(",")[0]
            tx_id = line.split(",")[1].strip()
            count = redis_client.get(tx_id)
            d.write(f"{tx_id},{count}\n")


if __name__ == "__main__":
    main()
