import os
import redis

REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)
REDIS_DB_NUM = os.getenv('REDIS_DB_NUM', 0)


def connect_to_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_NUM)
