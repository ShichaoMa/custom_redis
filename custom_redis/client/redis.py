# -*- coding:utf-8 -*-
"""redis_client"""
import json
import pickle
import time
import errno
import argparse

from copy import deepcopy
from socket import socket

from .functions import CMD_DICT
from .errors import RedisArgumentError, RedisError
from .utils import SafeList, func_name_wrapper, handle_safely,\
    default_recv, default_send, escape, unescape

FORMAT = b"%s#-*-#%s#-*-#1"


class Redis(object):

    def __init__(self, host="localhost", port=6379, timeout=30):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.redis_conn = None
        self.setup()

    def setup(self):
        self.close()
        self.redis_conn = socket()
        self.redis_conn.connect((self.host, self.port))
        self.redis_conn.settimeout(self.timeout)

    def __getattr__(self, name):
        return func_name_wrapper(name)(self._execute_cmd)

    def _execute_cmd(self, *args, **kwargs):
        args = SafeList(args)
        func_name = kwargs["_name"]
        properties = CMD_DICT[func_name]
        default_value = deepcopy(properties.get("default", []))
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
            sub = ""
            args = properties.get("args")
            default_args = SafeList(deepcopy(properties.get("default", [])))
            error_msg = "%%s haven't got enough arguments, need %s argument%s named %%s. "%(
                len(args), "" if len(args) == 1 else "s")
            for arg in reversed(args):
                sub = "%s%s"%(arg, (
                    ": default %s, "%default_args.pop(-1) if default_args else ", ")) + sub
            raise RedisArgumentError(error_msg%(func_name, sub[:-2]))
        if args:
            arguments += args
        return self._parse_result(FORMAT % (
            func_name.encode("utf-8"), b"%s<->%s"%escape(properties.get("send", default_send)(*arguments))), properties)

    def _parse_result(self, buf, properties={}):
        count = 0
        result = b""
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
            if not recv or recv.endswith(b"\r\n\r\n"):
                break
        result = result
        a = result.split(b"#-*-#")
        code, info, data = a
        data = data[:-4]
        if code == b"200":
            return handle_safely(properties.get("recv", default_recv))(unescape(data))
        elif code == b"502":
            return properties.get("result", data)
        else:
            raise RedisError(b"%s:%s, data: %s"%(code, info, data))

    def keys(self, pattern="*", *args):
        return self._parse_result(FORMAT%(b"keys", b"%s<->%s"%escape((pattern, b""))), {"recv":pickle.loads})

    def type(self, key, *args):
        return self._parse_result(FORMAT % (b"type", b"%s<->%s" % escape((key, b""))))

    def delete(self, key, *args):
        return self._parse_result(FORMAT % (b"delete", b"%s<->%s" % escape((key, b""))))

    def expire(self, key, seconds, *args):
        return self._parse_result(FORMAT % (b"expire", b"%s<->%s" % escape((key, str(seconds)))))

    def ttl(self, key, *args):
        return self._parse_result(FORMAT % (b"ttl", b"%s<->%s" % escape((key, ""))), {"recv":int})

    def flushall(self, *args):
        return self._parse_result(FORMAT % (b"flushall", b"<->"))

    def close(self):
        if self.redis_conn:
            try:
                self.redis_conn.close()
            except Exception:
                pass


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
        FORMAT.replace(b"1", b"0")
    if args.json:
        mapping = [json.loads(args.args[0])]
        result = getattr(r, args.cmd)(*(keys + mapping))
    else:
        result = getattr(r, args.cmd)(*(keys + args.args))
    if result != None:
        print(result)

    r.close()


if __name__ == "__main__":
    start_client()

