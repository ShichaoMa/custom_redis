# -*- coding:utf-8 -*-
import traceback
from Queue import Empty
from functools import wraps
from errors import ClientClosed


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


def stream_wrapper(func):
    """
    处理异常的装饰器
    :param func: 装饰的函数
    :return: 装饰好的函数
    """

    @wraps(func)
    def wrapper(*args):

        self, stream, w_lst, r_lst, server = args
        is_error = None
        is_closed = None
        try:
            return func(*args)
        except ClientClosed:
            is_closed = True
            is_error = True
        except Exception:
            self.logger.info(traceback.format_exc())
            is_error = True
        finally:
            if is_error or server is None:
                # 只有send的时候，server才会为空
                # 也就是说出了异常，或者send时，才会做下面的操作
                if stream in w_lst:
                    del w_lst[stream]
                # 如果每个socket属性元组中keep-alive 不为空，则不关闭stream
                if (stream in r_lst and (len(r_lst[stream]) != 3 or not r_lst[stream][2])) or is_closed:
                    del r_lst[stream]
                    try:
                        stream.close()
                    except:
                        pass

    return wrapper


def cmd_wrapper(func):
    @wraps(func)
    def wrapper(*args):

        self, k, v, instance = args
        try:
            self.key = k
            instance.datas[k] = self
            self.logger.info("process in method %s" % func.__name__)
            return self.format_response(200, "success", func(*args))
        except (Empty, KeyError):
            if not self.data:
                del instance.datas[k]
            self.logger.error(traceback.format_exc())
            return self.format_response(502, "Empty", "")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            return self.format_response(503, e.__class__.__name__.lower(), v)

    return wrapper