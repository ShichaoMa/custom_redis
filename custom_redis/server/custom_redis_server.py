# -*- coding:utf-8 -*-
"""redis_server"""
import os
import time
import errno
import socket
import select
import fnmatch
import traceback

from argparse import ArgumentParser
from threading import Thread

from multi_thread_closing import MultiThreadClosing

from errors import MethodNotExist, ClientClosed
from utils import stream_wrapper
from default_data_types import *


class CustomRedis(MultiThreadClosing):
    name = "redis_server"
    default = {"str": StrStore, "hash": HashStore, "set": SetStore, "zset": ZsetStore, "list": ListStore}

    def __init__(self, host, port):
        MultiThreadClosing.__init__(self)
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
        if os.path.exists("redis_data.db"):
            lines = open("redis_data.db").read().split("fdfsafafdsfsfdsfafdff")
            if lines:
                for line in lines:
                    self.load(line)

    def load(self, line):
        try:
            if line:
                key, expire_time, val = line.split("1qazxsw23edc")
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
        self.logger.debug("listen  to %s:%s"%(host, port))
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
        stream = open("redis_data.db", "w")
        self.logger.info("persist datas...")
        for key, val in self.datas.items():
            stream.write(key)
            stream.write('1qazxsw23edc%s' % self.expire_keys.get(key, ""))
            stream.write('1qazxsw23edc')
            val.persist(stream)
        stream.close()

    @stream_wrapper
    def send(self, w, w_lst, r_lst, server):
        # item会被保存在w_lst相应的val中
        item = w_lst[w]
        self.logger.debug("start to send item %s to %s:%s" % ((item,) + r_lst[w][:2]))
        w.send(item)

    @stream_wrapper
    def recv(self, r, w_lst, r_lst, server):
        if r is server:
            client, adr = r.accept()
            self.logger.debug("get connection from %s:%s" % adr)
            # 将新收到的socket设置为非阻塞， 并将其保存在r_lst中
            client.setblocking(0)
            r_lst[client] = adr
        else:
            self.logger.debug("start to recv data from %s:%s" % r_lst[r][:2])
            received = self._recv(r)
            if received:
                cmd, data, keep = received.split("#-*-#")
                key, val = data.split("<->")
                # 根据指令生成item返回结果
                try:
                    item = (getattr(self.datas.get(key, None), cmd, None) or getattr(self, cmd))(key, val, self)
                except MethodNotExist:
                    item = "404#-*-#Method Not Found#-*-#\r\n\r\n"
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
        return "%s#-*-#%s#-*-#%s\r\n\r\n" % ("200", "success",
                                     json.dumps(filter(lambda x: fnmatch.fnmatch(x, k), self.datas.keys())))

    def expire(self, k, v, instance):
        self.expire_keys[k] = int(time.time() + int(v))
        return "200#-*-#success#-*-#\r\n\r\n"

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

    @classmethod
    def parse_args(cls):
        parser = ArgumentParser()
        parser.add_argument("--host", dest="host", help="host", default="127.0.0.1")
        parser.add_argument("-p", "--port", type=int, dest="port", help="port", default=7777)
        return cls(**vars(parser.parse_args()))


def start_server():
    cr = CustomRedis.parse_args()
    cr.set_logger()
    cr.start()


if __name__ == "__main__":
    start_server()
