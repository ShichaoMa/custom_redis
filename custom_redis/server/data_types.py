# -*- coding:utf-8 -*-
import json
import random

from .errors import Empty
from .zset import SortedSet
from .bases import DataStore


class ZsetStore(DataStore):

    data_type = SortedSet

    def zadd(self, k, v, instance):
        k, v = [i for i in self._parses(v).items()][0]
        self.data.zadd(v, int(k))

    def zpop(self, k, v, instance):
        return self.data.zpop(v)

    def zcard(self, k, v, instance):
        return self.data.zcard


class ListStore(DataStore):

    data_type = list

    def lpop(self, k, v, instance):
        if self.data:
            return self.data.pop(0)
        else:
            raise Empty

    def rpush(self, k, v, instance):
        self.data.append(v)

    def llen(self, k, v, instance):
        return len(self.data)


class StrStore(DataStore):

    data_type = str

    def add(self, k, v, instance):
        self.data += v

    def slice(self, k, v, instance):
        return eval("self.data[%s]" % v)

    def set(self, k, v, instance):
        self.data = v

    def get(self, k, v, instance):
        return self.data


class SetStore(DataStore):

    data_type = set

    def sadd(self, k, v, instance):
        self.data.add(v)

    def scard(self, k, v, instance):
        return len(self.data)

    def smembers(self, k, v, instance):
        return json.dumps(list(self.data))

    def srem(self, k, v, instance):
        values = self._parses(v)
        for value in values:
            self.data.remove(value)

    def sismember(self, k, v, instance):
        return v in self.data

    def srchoice(self, k, v, instance):
        return random.choice(list(self.data))


class HashStore(DataStore):

    data_type = dict

    def hset(self, k, v, instance):
        self.data.update(self._parses(v))

    def hget(self, k, v, instance):
        return self.data[v]

    def hmset(self, k, v, instance):
        k_vs = self._parses(v)
        self.data.update(dict(k_vs))

    def hmget(self, k, v, instance):
        ks = self._parses(v)
        return json.dumps(dict(filter(lambda x: x[0] in ks, self.data.items())))

    def hgetall(self, k, v, instance):
        return json.dumps(self.data)

    def hincrby(self, k, v, instance):
        k_vs = self._parses(v)
        k = list(k_vs.keys())[0]
        v = k_vs[k]
        self.data[k] = int(self.data.get(k, 0)) + int(v)



