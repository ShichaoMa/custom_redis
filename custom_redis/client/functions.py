# -*- coding:utf-8 -*-
from .utils import safe_loads, safe_dumps


CMD_DICT = {
    "set": {
        "args": ["name", "value"],
        "send": lambda *args: (args[0], args[1]),
    },
    "get": {
        "args": ["name",],
    },
    "hget": {
        "args": ["name", "key"],
        "send": lambda *args: (args[0], args[1]),
    },
    "hset": {
        "args": ["name", "key", "value"],
        "send": lambda *args: (args[0], safe_dumps(dict([args[1:]]))),
    },
    "hmset": {
        "args": ["name", "mapping"],
        "send": lambda *args: (args[0], safe_dumps(args[1])),
    },
    "hmget": {
        "args": ["name", "key"],
        "send": lambda *args: (args[0], safe_dumps(args[1:])),
        "recv": lambda data:safe_loads(data),
    },
    "hgetall": {
        "args": ["name"],
        "recv": lambda data: safe_loads(data),
    },
    "hincrby": {
        "args": ["name", "key", "value"],
        "send": lambda *args: (args[0], safe_dumps(dict([args[1:]]))),
        "default": [1]
    },
    "zadd": {
        "args": ["name", "value", "key"],
        "send": lambda *args: (args[0], safe_dumps(dict([reversed(args[1:])]))),
    },
    "zpop": {
        "args": ["name", "withscore"],
        "send": lambda *args: (args[0], (True if args[1] in [True, "withscore"] else b"")),
        "recv": lambda data:safe_loads(data),
        "default": [False]
    },
    "zcard": {
        "args": ["name"],
        "recv": lambda x:int(x),
        "result": 0
    },
    "lpop": {
        "args": ["name"],
    },
    "rpush": {
        "args": ["name", "value"],
        "send": lambda *args: (args[0], args[1]),
    },
    "llen": {
        "args": ["name"],
        "recv": lambda x: int(x),
        "result": 0
    },
    "scard": {
        "args": ["name"],
        "recv": lambda x: int(x),
        "result": 0
    },
    "sadd": {
        "args": ["name", "value"],
        "send": lambda *args: (args[0], args[1]),
    },
    "srem": {
        "args": ["name", "values"],
        "send": lambda *args: (args[0], safe_dumps(args[1:])),
    },
    "srchoice": {
        "args": ["name"],
    },
    "smembers": {
        "args": ["name"],
        "recv": lambda x: set(safe_loads(x)),
    },
    "sismember": {
        "args": ["name", "value"],
        "send": lambda *args: (args[0], args[1]),
        "recv": lambda x: eval(x),
    },
}
