# -*- coding:utf-8 -*-
import pickle
import random

from .errors import Empty
from .zset import SortedSet
from .bases import DataStore


class ZsetStore(DataStore):

    data_type = SortedSet

    def zadd(self, k, v, instance):
        k, v = [i for i in pickle.loads(v).items()][0]
        self.data.zadd(v, int(k))

    def zpop(self, k, v, instance):
        return pickle.dumps(self.data.zpop(v))

    def zcard(self, k, v, instance):
        return str(self.data.zcard).encode("utf-8")


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
        return str(len(self.data)).encode("utf-8")


class StrStore(DataStore):

    data_type = str

    def add(self, k, v, instance):
        self.data += v

    def slice(self, k, v, instance):
        return eval(b"self.data[%s]" % v)

    def set(self, k, v, instance):
        self.data = v

    def get(self, k, v, instance):
        return self.data


class SetStore(DataStore):

    data_type = set

    def sadd(self, k, v, instance):
        self.data.add(v)

    def scard(self, k, v, instance):
        return str(len(self.data)).encode("utf-8")

    def smembers(self, k, v, instance):
        return pickle.dumps(list(self.data))

    def srem(self, k, v, instance):
        values = pickle.loads(v)
        for value in values:
            self.data.remove(bytes(str(value), encoding="utf-8"))

    def sismember(self, k, v, instance):
        return str(v in self.data).encode("utf-8")

    def srchoice(self, k, v, instance):
        return random.choice(list(self.data))


class HashStore(DataStore):

    data_type = dict

    def hset(self, k, v, instance):
        self.data.update(pickle.loads(v))

    def hget(self, k, v, instance):
        if isinstance(v, bytes):
            v = v.decode("utf-8")
        data = self.data[v]
        if not isinstance(data, bytes):
            data = str(data).encode("utf-8")
        return data

    def hmset(self, k, v, instance):
        k_vs = pickle.loads(v)
        self.data.update(dict(k_vs))

    def hmget(self, k, v, instance):
        ks = pickle.loads(v)
        return pickle.dumps(dict(filter(lambda x: x[0] in ks, self.data.items())))

    def hgetall(self, k, v, instance):
        return pickle.dumps(self.data)

    def hincrby(self, k, v, instance):
        k_vs = pickle.loads(v)
        k = list(k_vs.keys())[0]
        v = k_vs[k]
        self.data[k] = int(self.data.get(k, 0)) + int(v)



