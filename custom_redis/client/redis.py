# -*- coding:utf-8 -*-
"""redis_client"""
import json
import time
import errno
import argparse

from socket import socket

from functions import CMD_DICT
from errors import RedisArgumentError, RedisError
from utils import SafeList, func_name_wrapper, handle_safely, default_recv, default_send

FORMAT = "%s#-*-#%s#-*-#1"


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
        result = ""
        try:
            self.redis_conn.send(buf)
        except Exception as e:
            if e.args[0] == errno.EPIPE and count < 3:
                self.setup()
                count += 1
                time.sleep(1)
            else:
                raise
        while True:
            recv = self.redis_conn.recv(1024000)
            if recv:
                result += recv
            if not recv or recv.endswith("\r\n\r\n"):
                break
        a = result.split("#-*-#")
        code, info, data = a
        data = data[:-4]
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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", dest="host", default="127.0.0.1")
    parser.add_argument("-p", "--port", dest="port", type=int, default=6379)
    parser.add_argument("-c", "--cmd", dest="cmd", required=True)
    parser.add_argument("args", nargs="*", default=[])
    parser.add_argument("-k", "--key", dest="key")
    parser.add_argument("-j", "--json", dest="json", action="store_true")
    parser.add_argument("--keep-alive", dest="keep_alive", action="store_true")
    return parser.parse_args()


def start_client():
    args = parse_args()
    r = Redis(args.host, args.port)
    keys = [args.key] if args.key else []
    if not args.keep_alive:
        FORMAT.replace("1", "0")
    if args.json:
        mapping = [json.loads(args.args[0])]
        result = getattr(r, args.cmd)(*(keys + mapping))
    else:
        result = getattr(r, args.cmd)(*(keys + args.args))
    if result != None:
        print result


if __name__ == "__main__":
    start_client()

