# -*- coding:utf-8 -*-
import pickle
import traceback


class SafeList(list):
    def pop(self, index=-1):
        try:
            return super(SafeList, self).pop(index)
        except IndexError:
            return None


def default_send(*args):
    """对于参数数量确定的，有且只有一个的情况，可以使用默认发送函数"""
    return args[0], b""


def default_recv(x):
    return x


def safe_loads(data):
    try:
        return pickle.loads(data)
    except ValueError:
        return data


def safe_dumps(data):
    try:
        return pickle.dumps(data)
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
        if not isinstance(data, bytes):
            data = str(data).encode("utf-8")
        data = data.replace(b"<->", b"1qaxsw234fds3gbhfvhtedfvfg").replace(b"#-*-#", b"jp0n988n80434nlj3pdf0909mn")
        d.append(data)
    return tuple(d)


def unescape(data):
    return data.replace(b"1qaxsw234fds3gbhfvhtedfvfg", b"<->").replace(b"jp0n988n80434nlj3pdf0909mn", b"#-*-#")

