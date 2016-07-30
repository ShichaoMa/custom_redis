# -*- coding:utf-8 -*-
from socket import socket
import json
import traceback
import argparse
import errno
import time


FORMAT = "%s#-*-#%s#-*-#1"


def default_send(*args):
    if len(args) == 1:
        return "%s<->"%tuple(args)
    else:
        return "%s<->%s"%tuple(args)


def default_recv(x):
    return x


def func_name_wrapper(name):
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            kwargs["_name"] = name
            return func(*args, **kwargs)
        return inner_wrapper
    return wrapper


def handle_safely(func):
    def wrapper(data):
        try:
            return func(data)
        except Exception:
            traceback.print_exc()
            return data
    return wrapper


class RedisError(Exception):
    pass


class RedisArgumentError(Exception):
    pass


class SafeList(list):
    def pop(self, index=-1):
        try:
            return super(SafeList, self).pop(index)
        except IndexError:
            return None


class Redis(object):

    def __init__(self, host="localhost", port=6379, timeout=30):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.redis_conn = None
        self.setup()

    def setup(self):
        if self.redis_conn:
            try:
                self.redis_conn.close()
            except:
                pass
        print "set up connections..."
        self.redis_conn = socket()
        self.redis_conn.connect((self.host, self.port))
        self.redis_conn.settimeout(self.timeout)


    def __getattr__(self, name):
        return func_name_wrapper(name)(self._execute_cmd)

    def _execute_cmd(self, *args, **kwargs):
        args = SafeList(args)
        func_name = kwargs["_name"]
        properties = CMD_DICT[func_name]
        default_value = properties.get("default", [])
        try:
            arguments = []
            for arg in properties["args"]:
                kwarg = kwargs.pop(arg, None)
                if kwarg is None:
                    kwarg = args.pop(0)
                    if kwarg is None:
                        kwarg = default_value.pop(0)
                arguments.append(kwarg)
        except IndexError:
            raise RedisArgumentError("haven't got enough arguments")
        if args:
            arguments += args
        return self._parse_result(FORMAT % (func_name, properties.get("send", default_send)(*arguments)), properties)

    def _parse_result(self, buf, properties={}):
        count = 0
        result = None
        while True:
            try:
                self.redis_conn.send(buf)
                result = self.redis_conn.recv(102400)
            except Exception as e:
                if e.args[0] == errno.EPIPE and count < 3:
                    self.setup()
                    count += 1
                    time.sleep(1)
                else:
                    raise
            else:
                break
        code, info, data = result.split("#-*-#")
        if code == "200":
            return handle_safely(properties.get("recv", default_recv))(data)
        elif code == "502":
            return properties.get("result", data)
        else:
            raise RedisError("%s:%s, %s"%(code, info, data))

    def keys(self, pattern="*"):
        return self._parse_result(FORMAT%("keys", "%s<->"%pattern), {"recv":json.loads})

    def type(self, key):
        return self._parse_result(FORMAT % ("type", "%s<->" % key))

    def delete(self, key):
        return self._parse_result(FORMAT % ("delete", "%s<->" % key))

    def expire(self, key, seconds):
        return self._parse_result(FORMAT % ("expire", "%s<->%s" % (key, seconds)))

    def ttl(self, key):
        return self._parse_result(FORMAT % ("ttl", "%s<->" % key))

    def flushall(self):
        return self._parse_result(FORMAT % ("flushall", "<->"))


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
}

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cmd", dest="cmd", required=True)
    parser.add_argument("args", nargs="*", default=[])
    parser.add_argument("-k", "--key", dest="key")
    parser.add_argument("-j", "--json", dest="json", action="store_true")
    return parser.parse_args()

if __name__ == "__main__":
    r = Redis()
    args = parse_args()
    keys = [args.key] if args.key else []
    if args.json:
        mapping = [json.loads(args.args[0])]
        result = getattr(r, args.cmd)(*(keys + mapping))
    else:
        result = getattr(r, args.cmd)(*(keys + args.args))
    if result:
        print result

