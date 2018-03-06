# -*- coding:utf-8 -*-
"""这里定义一些框架基类和元类"""
import types
import pickle

from toolkit.singleton import SingletonABCMeta

from .utils import data_cmd_wrapper, common_cmd_wrapper


class Meta(type):
    """元类基类，给方法增加装饰器"""
    wrapper = None

    def __new__(mcs, name, bases, properties):
        for k, v in properties.items():
            if isinstance(v, types.FunctionType):
                # 由于这个方法会被继承，通过提供不同的wrapper函数来做不同的包装， RedisMeta没有提供，所以不包装
                properties[k] = mcs.wrapper(v) if mcs.wrapper else v
        return super().__new__(mcs, name, bases, properties)


class StoreMeta(Meta):
    """数据类专用元类"""
    wrapper = staticmethod(data_cmd_wrapper)


class RedisCommandMeta(Meta, SingletonABCMeta):
    """通用函数类专用元类"""
    wrapper = staticmethod(common_cmd_wrapper)


class RedisMeta(RedisCommandMeta):
    """Redis类专用元类"""
    wrapper = None

    def __new__(mcs, *args, **kwargs):
        # 这个地方非常诡异， 如果通过type(*args)来组建类，类会使用CommonCmdMeta来创建，可能原因是通过type返回的类是type创建的
        # 当使用默认type创建时，python认为该类没有指定元类，所以继续调用了父类的元类进行创建。
        # 通过type.__new__则会使用RedisMeta创建
        return super().__new__(mcs, *args)

    def __init__(cls, *args, **kwargs):
        # 当创建的实例(在这里是类)不是RedisMeta类型时，比如通过type()直接返回，__init__不会被调用。
        pass


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
