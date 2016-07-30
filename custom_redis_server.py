# -*- coding:utf-8 -*-
import socket
import fnmatch
import select
import traceback
import pickle
import errno
import json
import random
import os
import time
from copy import deepcopy
from functools import wraps
from Queue import Queue, Empty
from argparse import ArgumentParser
from threading import Thread
from log_utils import SignalLogger
from types import GeneratorType
from zset import SortedSet


class SortedDict(dict):
    """
    A dictionary that keeps its keys in the order in which they're inserted.
    """
    def __new__(cls, *args, **kwargs):
        instance = super(SortedDict, cls).__new__(cls, *args, **kwargs)
        instance.keyOrder = []
        return instance

    def __init__(self, data=None):
        if data is None:
            data = {}
        elif isinstance(data, GeneratorType):
            # Unfortunately we need to be able to read a generator twice.  Once
            # to get the data into self with our super().__init__ call and a
            # second time to setup keyOrder correctly
            data = list(data)
        super(SortedDict, self).__init__(data)
        if isinstance(data, dict):
            self.keyOrder = data.keys()
        else:
            self.keyOrder = []
            seen = set()
            for key, value in data:
                if key not in seen:
                    self.keyOrder.append(key)
                    seen.add(key)

    def __deepcopy__(self, memo):
        return self.__class__([(key, deepcopy(value, memo))
                               for key, value in self.iteritems()])

    def __setitem__(self, key, value):
        if key not in self:
            self.keyOrder.append(key)
        super(SortedDict, self).__setitem__(key, value)

    def __delitem__(self, key):
        super(SortedDict, self).__delitem__(key)
        self.keyOrder.remove(key)

    def __iter__(self):
        return iter(self.keyOrder)

    def pop(self, k, *args):
        result = super(SortedDict, self).pop(k, *args)
        try:
            self.keyOrder.remove(k)
        except ValueError:
            # Key wasn't in the dictionary in the first place. No problem.
            pass
        return result

    def popitem(self):
        result = super(SortedDict, self).popitem()
        self.keyOrder.remove(result[0])
        return result

    def items(self):
        return zip(self.keyOrder, self.values())

    def iteritems(self):
        for key in self.keyOrder:
            yield key, self[key]

    def keys(self):
        return self.keyOrder[:]

    def iterkeys(self):
        return iter(self.keyOrder)

    def values(self):
        return map(self.__getitem__, self.keyOrder)

    def itervalues(self):
        for key in self.keyOrder:
            yield self[key]

    def update(self, dict_):
        for k, v in dict_.iteritems():
            self[k] = v

    def setdefault(self, key, default):
        if key not in self:
            self.keyOrder.append(key)
        return super(SortedDict, self).setdefault(key, default)

    def value_for_index(self, index):
        """Returns the value of the item at the given zero-based index."""
        return self[self.keyOrder[index]]

    def insert(self, index, key, value):
        """Inserts the key, value pair before the item with the given index."""
        if key in self.keyOrder:
            n = self.keyOrder.index(key)
            del self.keyOrder[n]
            if n < index:
                index -= 1
        self.keyOrder.insert(index, key)
        super(SortedDict, self).__setitem__(key, value)

    def copy(self):
        """Returns a copy of this object."""
        # This way of initializing the copy means it works for subclasses, too.
        obj = self.__class__(self)
        obj.keyOrder = self.keyOrder[:]
        return obj

    def __repr__(self):
        """
        Replaces the normal dict.__repr__ with a version that returns the keys
        in their sorted order.
        """
        return '{%s}' % ', '.join(['%r: %r' % (k, v) for k, v in self.items()])

    def clear(self):
        super(SortedDict, self).clear()
        self.keyOrder = []


def stream_wrapper(func):
    """
    处理异常的装饰器
    :param func: 装饰的函数
    :return: 装饰好的函数
    """

    @wraps(func)
    def wrapper(*args):

        self, stream, w_lst, r_lst, server = args
        is_error = None
        is_closed = None
        try:
            return func(*args)
        except ClientClosed:
            is_closed = True
            is_error = True
        except Exception:
            self.logger.info(traceback.format_exc())
            is_error = True
        finally:
            if is_error or server is None:
                # 只有send的时候，server才会为空
                # 也就是说出了异常，或者send时，才会做下面的操作
                if stream in w_lst:
                    del w_lst[stream]
                # 如果每个socket属性元组中keep-alive 不为空，则不关闭stream
                if (stream in r_lst and (len(r_lst[stream]) != 3 or not r_lst[stream][2])) or is_closed:
                    del r_lst[stream]
                    try:
                        stream.close()
                    except:
                        pass

    return wrapper


def cmd_wrapper(func):
    @wraps(func)
    def wrapper(*args):

        self, k, v, instance = args
        try:
            self.key = k
            instance.datas[k] = self
            self.logger.info("process in method %s" % func.__name__)
            return self.format_response(200, "success", func(*args))
        except (Empty, KeyError):
            if not self.data:
                del instance.datas[k]
            self.logger.error(traceback.format_exc())
            return self.format_response(502, "Empty", "")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            return self.format_response(503, e.__class__.__name__.lower(), v)

    return wrapper


class DataStore(object):
    def __init__(self, logger, data=None):
        self.logger = logger
        self.data = data or self.data_type()

    @classmethod
    def from_redis(cls, redis):
        return cls(redis.logger)

    def format_response(self, code, info, data):
        if data is None:
            data = ""
        return "%s#-*-#%s#-*-#%s" % (code, info, data)

    def persist(self, stream):
        stream.write(pickle.dumps(self.data))
        stream.write("fdfsafafdsfsfdsfafdff")

    def _parses(self, v):
        return json.loads(v)

    @classmethod
    def loads(cls, val):
        if isinstance(val, cls.data_type):
            return cls


class ZsetStore(DataStore):
    data_type = SortedSet

    @cmd_wrapper
    def zadd(self, k, v, instance):
        k, v = self._parses(v).items()[0]
        self.data.zadd(v, int(k))

    @cmd_wrapper
    def zpop(self, k, v, instance):
        return self.data.zpop()

    @cmd_wrapper
    def zcard(self, k, v, instance):
        return self.data.zcard


class QueueStore(DataStore):
    data_type = Queue

    @cmd_wrapper
    def pop(self, k, v, instance):
        return self.data.get_nowait()

    @cmd_wrapper
    def push(self, k, v, instance):
        self.data.put(v)


class StrStore(DataStore):
    data_type = str

    @cmd_wrapper
    def add(self, k, v, instance):
        self.data += v

    @cmd_wrapper
    def slice(self, k, v, instance):
        return eval("self.data[%s]" % v)

    @cmd_wrapper
    def set(self, k, v, instance):
        self.data = v

    @cmd_wrapper
    def get(self, k, v, instance):
        return self.data


class SetStore(DataStore):
    data_type = set

    @cmd_wrapper
    def sadd(self, k, v, instance):
        self.data.add(v)

    @cmd_wrapper
    def smembers(self, k, v, instance):
        return len(self.data)

    @cmd_wrapper
    def remove(self, k, v, instance):
        self.data.remove(v)

    @cmd_wrapper
    def rchoice(self, k, v, instance):
        return random.choice(list(self.data))


class HashStore(DataStore):
    data_type = dict

    @cmd_wrapper
    def hset(self, k, v, instance):
        self.data.update(self._parses(v))

    @cmd_wrapper
    def hget(self, k, v, instance):
        return self.data[v]

    @cmd_wrapper
    def hmset(self, k, v, instance):
        k_vs = self._parses(v)
        self.data.update(dict(k_vs))

    @cmd_wrapper
    def hmget(self, k, v, instance):
        ks = self._parses(v)
        return json.dumps(dict(filter(lambda x: x[0] in ks, self.data.items())))

    @cmd_wrapper
    def hgetall(self, k, v, instance):
        return json.dumps(self.data)

    @cmd_wrapper
    def hincrby(self, k, v, instance):
        k_vs = self._parses(v)
        k = k_vs.keys()[0]
        v = k_vs[k]
        self.data[k] = int(self.data.get(k, 0)) + int(v)


class MethodNotExist(Exception):
    pass


class ClientClosed(Exception):
    pass


class CustomRedis(SignalLogger):
    default = {}

    def __init__(self, settings, host, port):
        self.name = "redis_server"
        super(SignalLogger, self).__init__(settings)
        self.host = host
        self.port = port
        self.data_type = {}
        self.datas = {}
        self.expire_keys = {}
        self.open()
        self.data_type.update(self.default)

    def install(self, **kwds):
        self.data_type.update(kwds)

    def setup(self):
        if os.path.exists("redis_data"):
            lines = open("redis_data").read().split("fdfsafafdsfsfdsfafdff")
            if lines:
                for line in lines:
                    self.load(line)

    def load(self, line):
        try:
            if line:
                key, expire_time, val = line.split("    ")
                val = pickle.loads(val)
                if len(self.data_type) < 2:
                    cls = self.data_type.values()[0].loads(val)
                else:
                    cls = reduce(lambda x, y:
                                 (x.loads(val) if hasattr(x, "loads") else None) or
                                 y.loads(val),
                                 self.data_type.values())
                if cls:
                    self.datas[key] = cls(self.logger, val)
                    if expire_time:
                        self.expire_keys[key] = int(expire_time)
        except Exception:
            self.logger.error(traceback.format_exc())

    def __getattr__(self, item):
        for k, v in self.data_type.items():
            attr = getattr(v, item, None)
            if attr:
                return getattr(v.from_redis(self), item)
        raise MethodNotExist

    def start(self):
        self.setup()
        daemaon_thread = Thread(target=self.poll)
        daemaon_thread.setDaemon(True)
        daemaon_thread.start()
        self.listen_request(self.host, self.port)

    def poll(self):
        while True:
            for key in self.expire_keys.keys():
                if self.expire_keys[key] < time.time():
                    del self.datas[key], self.expire_keys[key]
            time.sleep(1)

    def listen_request(self, host, port):
        """监听函数"""
        server = socket.socket()
        server.bind((host, port))
        server.listen(0)
        r_lst = {server: None}
        w_lst = {}
        # 若执行过程中出现异常r_lst中的server也被清掉，程序退出
        try:
            while self.alive and r_lst:
                readable, writable, _ = select.select(r_lst.keys(), w_lst.keys(), [], 0.1)
                for r in readable:
                    self.recv(r, w_lst, r_lst, server)
                for w in writable:
                    self.send(w, w_lst, r_lst, None)
                    # time.sleep(1)
        except select.error, e:
            if e.args[0] != 4:
                raise
        self.logger.info("exit...")

    def stop(self, *args):
        self.alive = False
        stream = open("redis_data", "w")
        self.logger.info("persist datas...")
        for key, val in self.datas.items():
            stream.write(key)
            stream.write('    %s' % self.expire_keys.get(key, ""))
            stream.write('    ')
            val.persist(stream)
        stream.close()

    @stream_wrapper
    def send(self, w, w_lst, r_lst, server):
        # item会被保存在w_lst相应的val中
        item = w_lst[w]
        self.logger.info("start to send item %s to %s:%s" % ((item,) + r_lst[w][:2]))
        w.send(item)

    @stream_wrapper
    def recv(self, r, w_lst, r_lst, server):
        if r is server:
            client, adr = r.accept()
            self.logger.info("get connection from %s:%s" % adr)
            # 将新收到的socket设置为非阻塞， 并将其保存在r_lst中
            client.setblocking(0)
            r_lst[client] = adr
        else:
            self.logger.info("start to recv data from %s:%s" % r_lst[r][:2])
            received = self._recv(r)
            if received:
                cmd, data, keep = received.split("#-*-#")
                key, val = data.split("<->")
                # 根据指令生成item返回结果
                try:
                    item = (getattr(self.datas.get(key, None), cmd, None) or getattr(self, cmd))(key, val, self)
                except MethodNotExist:
                    item = "404#-*-#Method Not Found#-*-#"
                # 将socket保存在w_lst中，并将keep-alive 标志保存在其val中
                w_lst[r] = item
                r_lst[r] = r_lst[r][:2] + (keep,)
            else:
                raise ClientClosed("closed")

    def _recv(self, r):
        msg = ""
        try:
            buf = r.recv(1024)
            while buf:
                msg += buf
                buf = r.recv(1024)
        except Exception, e:
            # 非阻塞异常直接返回
            if e.args[0] != errno.EAGAIN:
                # traceback.print_exc()
                pass
        self.logger.info("received massage is %s" % (msg or None))
        return msg

    def keys(self, k, v, instance):
        return "%s#-*-#%s#-*-#%s" % ("200", "success",
                                     json.dumps(filter(lambda x: fnmatch.fnmatch(x, k), self.datas.keys())))

    def expire(self, k, v, instance):
        self.expire_keys[k] = int(time.time() + int(v))
        return "200#-*-#success#-*-#"

    def type(self, k, v, instance):
        data = self.datas[k]
        return "200#-*-#success#-*-#%s"%data.__class__.__name__[:-5].lower()

    def ttl(self, k, v, instance):
        expire = self.expire_keys.get(k)
        if expire:
            expire = int(expire - time.time())
        else:
            expire = -1
        return "200#-*-#success#-*-#%d" % expire

    def delete(self, k, v, instance):
        try:
            del self.datas[k]
        except KeyError:
            pass
        return "200#-*-#success#-*-#"

    def flushall(self, k, v, instance):
        self.datas = {}
        return "200#-*-#success#-*-#"

    @classmethod
    def parse_args(cls):
        parser = ArgumentParser()
        parser.add_argument("-s", "--settings", dest="settings", help="settings", default="settings.py")
        parser.add_argument("--host", dest="host", help="host", default="192.168.200.58")
        parser.add_argument("-p", "--port", type=int, dest="port", help="port", default=7777)
        return cls(**vars(parser.parse_args()))


if __name__ == "__main__":
    cr = CustomRedis.parse_args()
    cr.install(str=StrStore, hash=HashStore, set=SetStore, zset=ZsetStore)
    cr.start()
