# -*- coding:utf-8 -*-
"""这里定义一些框架基类和元类"""
import types
import pickle
import traceback

from functools import wraps

from .errors import Empty
from .utils import format_response


class Meta(type):
    """元类基类，给方法增加装饰器"""
    wrapper = None

    def __new__(mcs, name, bases, properties):
        for k, v in properties.items():
            if isinstance(v, types.FunctionType):
                # 由于这个方法会被继承，通过提供不同的wrapper函数来做不同的包装，
                # RedisMeta没有提供，所以不包装
                properties[k] = mcs.wrapper(v) if mcs.wrapper else v
        return super().__new__(mcs, name, bases, properties)


class StoreMeta(Meta):
    """数据类专用元类"""
    @staticmethod
    def wrapper(func):
        @wraps(func)
        def inner(*args):
            self, k, v, instance = args
            try:
                self.key = k
                instance.datas[k] = self
                self.logger.info("process in method %s" % func.__name__)
                return format_response(b"200", b"success", func(*args))
            except (Empty, KeyError):
                if not self.data:
                    del instance.datas[k]
                self.logger.error(traceback.format_exc())
                return format_response(b"502", b"Empty", b"")
            except Exception as e:
                self.logger.error(traceback.format_exc())
                return format_response(
                    b"503", "{}:{}".format(
                        e.__class__.__name__.lower(), e).encode(), v)
        return inner


class RedisCommandMeta(Meta):
    """通用函数类专用元类"""
    @staticmethod
    def wrapper(func):
        @wraps(func)
        def inner(*args):
            self, k, v, instance = args
            try:
                return func(*args)
            except Exception as e:
                self.logger.error(traceback.format_exc())
                return format_response(
                    b"503", "{}:{}".format(
                        e.__class__.__name__.lower(), e).encode(), v)
        return inner


class DataCommonCommand(object):
    """数据公共方法"""
    data_type = None

    def __init__(self, logger, data=None):
        self.logger = logger
        self.data = data or self.data_type()

    @classmethod
    def from_redis(cls, redis):
        return cls(redis.logger)

    def persist(self, stream):
        stream.write(pickle.dumps(self.data))
        stream.write(b"fdfsafafdsfsfdsfafdff")

    @classmethod
    def loads(cls, val):
        if isinstance(val, cls.data_type):
            return cls


class DataStore(DataCommonCommand, metaclass=StoreMeta):
    """数据基类"""
    pass
