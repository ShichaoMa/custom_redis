# -*- coding:utf-8 -*-
import pickle
import random
import json

from zset import SortedSet
from Queue import Queue, Empty
from utils import cmd_wrapper


class DataStore(object):

    data_type = None

    def __init__(self, logger, data=None):
        self.logger = logger
        self.data = data or self.data_type()

    @classmethod
    def from_redis(cls, redis):
        return cls(redis.logger)

    def format_response(self, code, info, data):
        if data is None:
            data = ""
        return "%s#-*-#%s#-*-#%s\r\n\r\n" % (code, info, data)

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
        return self.data.zpop(v)

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


class ListStore(DataStore):
    data_type = list

    @cmd_wrapper
    def lpop(self, k, v, instance):
        if self.data:
            return self.data.pop(0)
        else:
            raise Empty

    @cmd_wrapper
    def rpush(self, k, v, instance):
        self.data.append(v)

    @cmd_wrapper
    def llen(self, k, v, instance):
        return len(self.data)


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
    def scard(self, k, v, instance):
        return len(self.data)

    @cmd_wrapper
    def smembers(self, k, v, instance):
        return json.dumps(list(self.data))

    @cmd_wrapper
    def srem(self, k, v, instance):
        values = self._parses(v)
        for value in values:
            self.data.remove(value)

    @cmd_wrapper
    def sismember(self, k, v, instance):
        return v in self.data

    @cmd_wrapper
    def srchoice(self, k, v, instance):
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
