# -*- coding:utf-8 -*-
"""这里定义一些框架基类和元类"""
import types
import pickle

from .utils import data_cmd_wrapper, common_cmd_wrapper


class Meta(type):
    """元类基类，给方法增加装饰器"""
    wrapper = None

    def __new__(typ, name, bases, properties):
        # 一些公共继承方法继承在DataStore里面，不需要被装饰，通过其基类来判断要构造的是否为DataStore类
        if bases[0] != object:
            for k, v in properties.items():
                if isinstance(v, types.FunctionType):
                    # 由于这个方法会被继承，通过提供不同的wrapper函数来做不同的包装， RedisMeta没有提供，所以不包装
                    properties[k] = typ.wrapper(v) if typ.wrapper else v
        return super(Meta, typ).__new__(typ, name, bases, properties)


class StoreMeta(Meta):
    """数据类专用元类"""
    wrapper = staticmethod(data_cmd_wrapper)


class CommonCmdMeta(StoreMeta):
    """通用函数类专用元类"""
    wrapper = staticmethod(common_cmd_wrapper)


class RedisMeta(CommonCmdMeta):
    """Redis类专用元类"""
    wrapper = None

    def __new__(typ, *args, **kwargs):
        # 这个地方非常诡异， 如果通过type(*args)来组建类，类会使用CommonCmdMeta来创建，可能原因是通过type返回的类是type创建的
        # 当使用默认type创建时，python认为该类没有指定元类，所以继续调用了父类的元类进行创建。
        # 通过type.__new__则会使用RedisMeta创建
        # 不通过super(RedisMeta, typ).__new__(typ, *args)创建，是因为这样会调用CommonCmdMeta.__new__，这不是我们想要的。
        # 这里我们不需要对CustomRedis类的函数进行包装操作，所以选择使用type.__new__创建
        return super(RedisMeta, typ).__new__(typ, *args)

    def __init__(cls, *args, **kwargs):
        # 当创建的实例(在这里是类)不是RedisMeta类型时，比如通过type()直接返回，__init__不会被调用。
        pass


class DataStore(object, metaclass=StoreMeta):
    """数据基类"""
    data_type = None

    def __init__(self, logger, data=None):
        self.logger = logger
        self.data = data or self.data_type()

    @classmethod
    def from_redis(cls, redis):
        return cls(redis.logger)

    @staticmethod
    def format_response(code, info, data):
        if data is None:
            data = b""
        return b"%s#-*-#%s#-*-#%s\r\n\r\n" % (code, info, data)

    def persist(self, stream):
        stream.write(pickle.dumps(self.data))
        stream.write(b"fdfsafafdsfsfdsfafdff")

    def _parses(self, v):
        return pickle.loads(v)

    @classmethod
    def loads(cls, val):
        if isinstance(val, cls.data_type):
            return cls

