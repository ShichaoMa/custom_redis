# -*- coding:utf-8 -*-
"""这里定义的是一些通用的方法"""
import time
import json
import fnmatch

from .bases import DataStore, CommonCmdMeta


class CommonCmd(DataStore, metaclass=CommonCmdMeta):
    """通用函数类"""
    expire_keys = {}

    def keys(self, k, v, instance):
        return "%s#-*-#%s#-*-#%s\r\n\r\n" % ("200", "success",
                                     json.dumps([x for x in self.datas.keys() if fnmatch.fnmatch(x, k)]))

    def expire(self, k, v, instance):
        if k in self.datas:
            self.expire_keys[k] = int(time.time() + int(v))
            return "200#-*-#success#-*-#\r\n\r\n"
        raise KeyError(k)

    def type(self, k, v, instance):
        data = self.datas[k]
        return "200#-*-#success#-*-#%s\r\n\r\n"%data.__class__.__name__[:-5].lower()

    def ttl(self, k, v, instance):
        expire = self.expire_keys.get(k)
        if expire:
            expire = int(expire - time.time())
        else:
            expire = -1
        return "200#-*-#success#-*-#%d\r\n\r\n" % expire

    def delete(self, k, v, instance):
        try:
            del self.datas[k]
        except KeyError:
            pass
        return "200#-*-#success#-*-#\r\n\r\n"

    def flushall(self, k, v, instance):
        self.datas = {}
        return "200#-*-#success#-*-#\r\n\r\n"
