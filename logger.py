# -*- coding:utf-8 -*-
import logging
import os
import sys
import errno
import time
import traceback
import copy
import datetime
from pythonjsonlogger import jsonlogger
from kafka import KafkaClient, SimpleProducer
from kafka.common import FailedPayloadsError
from functools import wraps
from cloghandler import ConcurrentRotatingFileHandler
from settings_wrapper import SettingsWrapper


settings = SettingsWrapper().load("default_settings.py", "default_settings.py")


def failedpayloads_wrapper(max_iter_times, _raise=False):

    def out_wrapper_methed(func):

        @wraps(func)
        def inner_wrapper_methed(*args):
            count = 0
            while count <= max_iter_times:
                try:
                    func(*args)
                    break
                except Exception, e:
                    if _raise and not isinstance(e, FailedPayloadsError):
                        raise e
                    count += 1
                    traceback.print_exc()
                    if count > max_iter_times and _raise:
                        raise
                    time.sleep(0.1)

        return inner_wrapper_methed

    return out_wrapper_methed


class LogFactory(object):

    _instance = None

    @classmethod
    def get_instance(self, **kwargs):
        if self._instance is None:
            self._instance = LogObject(**kwargs)

        return self._instance


class KafkaHandler(logging.Handler):

    def __init__(self, settings):
        self.client = KafkaClient(settings.get("KAFKA_HOSTS"))
        self.producer = SimpleProducer(self.client)
        self.producer.send_messages = failedpayloads_wrapper(5)(self.producer.send_messages)
        super(KafkaHandler, self).__init__()

    def emit(self, record):
        self.client.ensure_topic_exists(settings.get("TOPIC"))
        buf = self.formatter.format(record)
        self.producer.send_messages(settings.get("TOPIC"), buf)

    def close(self):
        self.acquire()
        super(KafkaHandler, self).close()
        self.client.close()
        self.release()


def extras_wrapper(self, item):

    def logger_func_wrapper(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            if self.level_dict[item.upper()] >= self.level_dict[self.log_level]:
                if len(args) > 2:
                    extras = args[1]
                else:
                    extras = kwargs.pop("extras", {})
                extras = self.add_extras(extras, item)
                return func(args[0], extra=extras)

        return wrapper

    return logger_func_wrapper


class LogObject(object):

    level_dict = {
        "DEBUG": 0,
        "INFO": 1,
        "WARN": 2,
        "WARNING": 2,
        "ERROR": 3,
        "CRITICAL": 4,
    }

    def __init__(self, json=False, name='scrapy-cluster', level='INFO',
                 format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                 propagate=False):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = propagate
        self.json = json
        self.name = name
        self.log_level = level
        self.format_string = format

    def set_handler(self, handler):
        handler.setLevel(logging.DEBUG)
        formatter = self._get_formatter(self.json)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self._check_log_level(self.log_level)
        self.logger.debug("Logging to %s"%handler.__class__.__name__)

    def __getattr__(self, item):
        if item.upper() in self.level_dict:
            return extras_wrapper(self, item)(getattr(self.logger, item))
        else:
            return super(LogObject, self).__getattr__(item)

    def _get_formatter(self, json):
        if json:
            return jsonlogger.JsonFormatter()
        else:
            return logging.Formatter(self.format_string)

    def _check_log_level(self, level):
        if level not in self.level_dict.keys():
            self.log_level = 'DEBUG'
            self.logger.warn("Unknown log level '%s', defaulting to DEBUG"%level)

    def add_extras(self, dict, level):
        my_copy = copy.deepcopy(dict)
        if 'level' not in my_copy:
            my_copy['level'] = level
        if 'timestamp' not in my_copy:
            my_copy['timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if 'logger' not in my_copy:
            my_copy['logger'] = self.name
        return my_copy


class Logger(object):

    name = "root"

    setting_wrapper = SettingsWrapper()

    def __init__(self, settings):
        self.settings = self.setting_wrapper.load(settings, "default_settings.py")
        self.set_logger()

    def set_logger(self, logger=None):
        if logger:
            self.logger = logger
            return
        my_level = self.settings.get('LOG_LEVEL', 'INFO')
        my_name = self.name
        my_output = self.settings.get('LOG_STDOUT', False)
        my_json = self.settings.get('LOG_JSON', True)
        my_dir = self.settings.get('LOG_DIR', 'logs')
        my_bytes = self.settings.get('LOG_MAX_BYTES', '10MB')
        my_file = "%s.log" % self.name
        my_backups = self.settings.get('LOG_BACKUPS', 5)
        to_kafka = self.settings.get("TO_KAFKA", False)
        self.logger = LogFactory.get_instance(json=my_json,
                                                    name=my_name,
                                                    level=my_level)
        if to_kafka:
            self.logger.set_handler(KafkaHandler(settings))
        elif my_output:
            self.logger.set_handler(logging.StreamHandler(sys.stdout))
        else:
            try:
                # try to make dir
                os.makedirs(my_dir)
            except OSError as exception:
                if exception.errno != errno.EEXIST:
                    raise
            self.logger.set_handler(
                ConcurrentRotatingFileHandler(
                    os.path.join(my_dir, my_file),
                    backupCount=my_backups,
                    maxBytes=my_bytes))


if __name__ == "__main__":
    # my_dir = settings.get("LOG_DIR")
    # try:
    #     os.makedirs(my_dir)
    # except OSError as exception:
    #     if exception.errno != errno.EEXIST:
    #         raise
    # logger = CustomLogFactory.get_instance(name="test_name")
    # logger.set_handler(
    #     ConcurrentRotatingFileHandler(
    #         os.path.join(my_dir, "test.log"),
    #         backupCount=5,
    #         maxBytes=10240))
    # logger.info("this is a log. ")
    #################################################
    # logger = CustomLogFactory.get_instance(name="test_name", json=True)
    # kafka_handler = KafkaHandler(settings)
    # logger.set_handler(kafka_handler)
    # logger.info("this is a log. ")
    #################################################
    # logger = CustomLogFactory.get_instance(name="test_name")
    # logger.set_handler(logging.StreamHandler(sys.stdout))
    # logger.info("this is a log. ")
    #################################################
    obj = Logger("defaut_settings.py")
    obj.logger.info("this is a log. ")
