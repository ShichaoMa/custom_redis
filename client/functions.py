# -*- coding:utf-8 -*-
import json


CMD_DICT = {
    "set": {
        "args": ["name", "value"],
        "send":lambda *args:"%s<->%s"%tuple(args),
    },
    "get": {
        "args": ["name",],
    },
    "hget":{
        "args":["name", "key"],
    },
    "hset":{
        "args": ["name", "key", "value"],
        "send":lambda *args:"%s<->%s"%(args[0], json.dumps(dict([args[1:]]))),
    },
    "hmset": {
        "args": ["name", "mapping"],
        "send":lambda *args:"%s<->%s"%(args[0], json.dumps(args[1])),
    },
    "hmget": {
        "args": ["name", "key"],
        "send": lambda *args:"%s<->%s"%(args[0], json.dumps(args[1:])),
        "recv": lambda data:json.loads(data),
    },
    "hgetall": {
        "args": ["name"],
        "send": lambda *args: "%s<->" % args[0],
        "recv": lambda data: json.loads(data),
    },
    "hincrby": {
        "args": ["name", "key", "value"],
        "send": lambda *args:"%s<->%s"%(args[0], json.dumps(dict([args[1:]]))),
        "default":[1]
    },
    "pop": {
        "args": ["name"],
        "recv": lambda data: json.loads(data),
    },
    "push": {
        "args": ["name", "value"],
        "send": lambda *args: "%s<->%s" % (args[0], json.dumps(args[1])),
    },
    "zadd": {
        "args": ["name", "key", "value"],
        "send": lambda *args: "%s<->%s" % (args[0], json.dumps(dict([args[1:]]))),
    },
    "zpop": {
        "args": ["name"],
        "send": lambda *args: "%s<->" % args[0],
    },
    "zcard": {
        "args": ["name"],
        "send": lambda *args: "%s<->" % args[0],
        "recv": lambda x:int(x),
        "result": 0
    },
    "lpop": {
        "args": ["name"],
        "send": lambda *args: "%s<->" % args[0],
    },
    "rpush": {
        "args": ["name", "value"],
        "send": lambda *args: "%s<->%s" % (args[0], args[1]),
    },
    "llen": {
        "args": ["name"],
        "send": lambda *args: "%s<->" % args[0],
        "recv": lambda x: int(x),
        "result": 0
    },
}
