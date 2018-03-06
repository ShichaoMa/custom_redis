# -*- coding:utf-8 -*-
"""
redis server
主线程使用select进程socket监听和任务处理
守护线程用来持久化数据和删除过期key
"""
import os
import sys
import time
import errno
import socket
import select
import pickle
import logging
import traceback

from logging import handlers
from functools import reduce
from argparse import ArgumentParser
from threading import Thread, RLock

from toolkit import cache_property
from toolkit.monitors import ParallelMonitor

from .bases import RedisMeta
from .utils import stream_wrapper
from .redis_command import RedisCommand
from .errors import MethodNotExist, ClientClosed
from .data_types import ZsetStore, StrStore, HashStore, SetStore, ListStore


class RedisServer(RedisCommand, ParallelMonitor, metaclass=RedisMeta):

    name = "redis_server"
    default_data_types = {"str": StrStore, "hash": HashStore, "set": SetStore, "zset": ZsetStore, "list": ListStore}
    expire_keys = None

    def __init__(self):
        ParallelMonitor.__init__(self)
        self.args = self.parse_args()
        self.host = self.args.get("host")
        self.port = self.args.get("port")
        # 所有数据类型
        self.data_type = {}
        # 所有数据实例
        self.datas = {}
        # 所有数据的过期时间
        self.expire_keys = {}
        self.lock = RLock()
        self.open()
        self.data_type.update(self.default_data_types)
        self.r_lst = {}
        self.w_lst = {}

    @cache_property
    def logger(self):
        logger = logging.getLogger(self.name)
        logger.setLevel(getattr(logging, self.args.get("log_level", "DEBUG")))
        if self.args.get("log_file"):
            handler = handlers.RotatingFileHandler(
                os.path.join(self.args.get("log_dir", "."), "%s.log" % self.name),
                maxBytes=10240000, backupCount=5)
        else:
            handler = logging.StreamHandler(sys.stdout)
        formater = logging.Formatter(self.args.get("log_format", "%(message)s"))
        handler.setFormatter(formater)
        logger.addHandler(handler)
        return logger

    def install(self, **kwds):
        self.data_type.update(kwds)

    def setup(self):
        if os.path.exists("redis_data.db"):
            lines = open("redis_data.db", "rb").read().split(b"fdfsafafdsfsfdsfafdff")
            if lines:
                self.logger.info("load datas...")
                for line in lines:
                    self.load(line)

    def load(self, line):
        # 加载数据类型
        try:
            if line:
                key, expire_time, val = line.split(b"1qazxsw23edc")
                val = pickle.loads(val)
                if len(self.data_type) < 2:
                    cls = list(self.data_type.values())[0].loads(val)
                else:
                    cls = reduce(lambda x, y:
                                 (x.loads(val) if hasattr(x, "loads") else None) or
                                 y.loads(val),
                                 self.data_type.values())
                if cls:
                    self.datas[key] = cls(self.logger, val)
                    if expire_time != b"-1":
                        self.expire_keys[key] = int(expire_time)
        except Exception:
            self.logger.error(traceback.format_exc())

    def __getattr__(self, item):
        # 遍历所有数据类型，找到数据类型对应方法并返回
        for k, v in self.data_type.items():
            attr = getattr(v, item, None)
            if attr:
                return getattr(v.from_redis(self), item)
        raise MethodNotExist

    def start(self):
        self.setup()
        poll_thread = Thread(target=self.poll)
        poll_thread.start()
        self.children.append(poll_thread)
        self.listen_request(self.host, self.port)

    def poll(self):
        """删除过期的Key, 删除空集合及持久化数据的守护线程"""
        t = time.time()
        while self.alive:
            # 每30秒持久化一次数据
            if time.time()-t > 30:
                t = time.time()
                self.persist()
            for key in list(self.expire_keys.keys()):
                if self.expire_keys[key] < time.time():
                    del self.datas[key], self.expire_keys[key]
            for key in [i for i in self.datas.keys()]:
                if not self.datas[key].data:
                    del self.datas[key]
            time.sleep(1)

    def listen_request(self, host, port):
        """监听函数"""
        self.logger.info("listen  to %s:%s"%(host, port))
        server = socket.socket()
        server.bind((host, port))
        server.listen(10)
        self.r_lst[server] = None
        # 若执行过程中出现异常r_lst中的server也被清掉，程序退出
        try:
            while self.alive and self.r_lst:
                readable, writable, _ = select.select(self.r_lst.keys(), self.w_lst.keys(), [], 0.1)
                for r in readable:
                    self.recv(r, self.w_lst, self.r_lst, server)
                for w in writable:
                    self.send(w, self.w_lst, self.r_lst, None)
        except select.error as e:
            if e.args[0] != 4:
                raise
        finally:
            self.persist()
            # 只关r_list的即可
            for i in self.r_lst.keys():
                try:
                    i.close()
                except Exception:
                    pass

    def persist(self, stream=None):
        with self.lock:
            with open("redis_data.db", "wb") as stream:
                self.logger.info("persist datas...")
                for key, val in self.datas.items():
                    stream.write(key)
                    stream.write(b'1qazxsw23edc%d' % self.expire_keys.get(key, -1))
                    stream.write(b'1qazxsw23edc')
                    val.persist(stream)

    @stream_wrapper
    def send(self, w, w_lst, r_lst, server):
        # item会被保存在w_lst相应的val中
        item = w_lst[w]
        self.logger.debug("start to send item %s to %s:%s" % (item, r_lst[w][0], r_lst[w][1]))
        w.send(item)

    @stream_wrapper
    def recv(self, r, w_lst, r_lst, server):
        if r is server:
            client, adr = r.accept()
            self.logger.debug("get connection from %s:%s" % (adr[0], adr[1]))
            # 将新收到的socket设置为非阻塞， 并将其保存在r_lst中
            client.setblocking(0)
            r_lst[client] = adr
        else:
            self.logger.debug("start to recv data from %s:%s" % (r_lst[r][0], r_lst[r][1]))
            received = self._recv(r)
            if received:
                cmd, data, keep = received.split(b"#-*-#")
                cmd = cmd.decode("utf-8")
                key, val = data.split(b"<->")
                # 根据指令生成item返回结果
                try:
                    method = getattr(self.datas.get(key, None), cmd, None) or getattr(self, cmd)
                    #method = getattr(self.datas.get(key, None), cmd, None)
                    # 数据类型的方法
                    if method and key in self.datas and method.__self__ != self and \
                                    self.datas[key].__class__ != method.__self__.__class__:
                        # 类型不符合
                        item = b"503#-*-#Type Not Format#-*-#\r\n\r\n"
                    else:
                        item = method(key, val, self)
                    # else:
                    #     # 通用方法
                    #     item = getattr(self, cmd)(key, val, self)
                except MethodNotExist:
                    item = b"404#-*-#Method Not Found#-*-#\r\n\r\n"
                # 将socket保存在w_lst中，并将keep-alive 标志保存在其val中
                w_lst[r] = item
                r_lst[r] = r_lst[r][:2] + (keep,)
            else:
                raise ClientClosed("closed")

    def _recv(self, r):
        msg = b""
        try:
            buf = r.recv(1024)
            while buf:
                msg += buf
                buf = r.recv(1024)
        except Exception as e:
            # 非阻塞异常直接返回
            if e.args[0] != errno.EAGAIN:
                pass
        self.logger.debug("received massage is %s" % (msg or None))
        return msg

    def parse_args(self):
        parser = ArgumentParser()
        parser.add_argument("--host", help="host", default="127.0.0.1")
        parser.add_argument("-p", "--port", type=int, help="port", default=6379)
        parser.add_argument("-lf", "--log-file", action="store_true", help="log to file, else log to stdout. " )
        parser.add_argument("-ld", "--log-dir", default=".")
        parser.add_argument("-ll", "--log-level", default="DEBUG", choices=["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"])
        parser.add_argument("--log-format", default="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
        return vars(parser.parse_args())


def start_server():
    RedisServer().start()


if __name__ == "__main__":
    start_server()
