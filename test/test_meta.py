from custom_redis.server.bases import RedisMeta
from custom_redis.server.redis_command import RedisCommand


class A(RedisCommand, metaclass=RedisMeta):
    pass

