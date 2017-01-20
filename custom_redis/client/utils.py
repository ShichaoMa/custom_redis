# -*- coding:utf-8 -*-
import json
import traceback


class SafeList(list):
    def pop(self, index=-1):
        try:
            return super(SafeList, self).pop(index)
        except IndexError:
            return None


def default_send(*args):
    """对于参数数量确定的，有且只有一个的情况，可以使用默认发送函数"""
    return args[0], ""


def default_recv(x):
    return x


def safe_loads(data):
    try:
        return json.loads(data)
    except ValueError:
        return data


def safe_dumps(data):
    try:
        return json.dumps(data)
    except ValueError:
        return data


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


def escape(datas):
    d = []
    for data in datas:
        if isinstance(data, str):
            data = data.replace("<->", "1qaxsw234fds3gbhfvhtedfvfg").replace("#-*-#", "jp0n988n80434nlj3pdf0909mn")
        d.append(data)
    return tuple(d)


def unescape(data):
    return data.replace("1qaxsw234fds3gbhfvhtedfvfg", "<->").replace("jp0n988n80434nlj3pdf0909mn", "#-*-#")

