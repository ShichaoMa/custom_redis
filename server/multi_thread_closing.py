# -*- coding:utf-8 -*-
import sys
import ctypes
import signal
import inspect
import logging


class MultiThreadClosing(object):

    alive = True
    name = "root"
    threads = []
    int_signal_count = 1

    def __init__(self):
        self.open()

    def set_logger(self, logger=None):
        if not logger:
            self.logger = logging.getLogger(self.name)
            self.logger.setLevel(10)
            self.logger.addHandler(logging.StreamHandler(sys.stdout))
        else:
            self.logger = logger
            self.name = logger.name

    def stop(self, *args):
        if self.int_signal_count > 1:
            self.logger.info("force to terminate all the threads...")
            for th in self.threads[:]:
                self.stop_thread(th)

        else:
            self.alive = False
            self.logger.info("close processor %s..." % self.name)
            self.int_signal_count += 1

    def open(self):
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def set_force_interrupt(self, thread):
        self.threads.append(thread)

    def _async_raise(self, name, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        self.logger.debug("stop thread %s. "%name)
        if res == 0:
            self.logger.error("invalid thread id")
            raise ValueError("invalid thread id")
        elif res != 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            self.logger.error("PyThreadState_SetAsyncExc failed")
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def stop_thread(self, thread):
        self.logger.info("stopping thread %s"%thread.getName())
        self._async_raise(thread.getName(), thread.ident, SystemExit)
        self.logger.info("stopping thread %s finish" % thread.getName())
        self.threads.remove(thread)
        self.logger.info("remove thread %s " % thread.getName())
