# -*- coding:utf-8 -*-
import traceback



class SafeList(list):
    def pop(self, index=-1):
        try:
            return super(SafeList, self).pop(index)
        except IndexError:
            return None


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

