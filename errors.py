# -*- coding:utf-8 -*-


class MethodNotExist(Exception):
    pass


class ClientClosed(Exception):
    pass


class RedisError(Exception):
    pass


class RedisArgumentError(Exception):
    pass