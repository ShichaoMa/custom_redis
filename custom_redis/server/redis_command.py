# -*- coding:utf-8 -*-
"""这里定义的是一些通用的方法"""
import time
import pickle
import fnmatch

from abc import abstractmethod

from .bases import RedisCommandMeta


class RedisCommand(object, metaclass=RedisCommandMeta):
    """通用函数类"""
    @property
    @abstractmethod
    def expire_keys(self):
        pass

    def keys(self, k, v, instance):
        return b"%s#-*-#%s#-*-#%s\r\n\r\n" % (b"200", b"success",
                                             pickle.dumps([x for x in self.datas.keys() if fnmatch.fnmatch(x, k)]))

    def expire(self, k, v, instance):
        if k in self.datas:
            self.expire_keys[k] = int(time.time() + int(v))
            return b"200#-*-#success#-*-#\r\n\r\n"
        raise KeyError(k)

    def type(self, k, v, instance):
        data = self.datas[k]
        return ("200#-*-#success#-*-#%s\r\n\r\n"%data.__class__.__name__[:-5].lower()).encode("utf-8")

    def ttl(self, k, v, instance):
        expire = self.expire_keys.get(k)
        if expire:
            expire = int(expire - time.time())
        else:
            expire = -1
        return ("200#-*-#success#-*-#%d\r\n\r\n" % expire).encode("utf-8")

    def delete(self, k, v, instance):
        try:
            del self.datas[k]
        except KeyError:
            pass
        return b"200#-*-#success#-*-#\r\n\r\n"

    def flushall(self, k, v, instance):
        self.datas = {}
        return b"200#-*-#success#-*-#\r\n\r\n"
