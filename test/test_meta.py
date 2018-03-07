from custom_redis.server.bases import RedisMeta


class A(object, metaclass=RedisMeta):
    pass

    