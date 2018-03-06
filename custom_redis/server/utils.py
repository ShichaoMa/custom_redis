# -*- coding:utf-8 -*-
import traceback

from functools import wraps

from .errors import ClientClosed, Empty


def stream_wrapper(func):
    """
    处理流异常的装饰器
    :param func:
    :return:
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
                if (stream in r_lst and (len(r_lst[stream]) != 3 or not int(r_lst[stream][2]))) or is_closed:
                    del r_lst[stream]
                    try:
                        stream.close()
                    except:
                        pass

    return wrapper


def data_cmd_wrapper(func):
    """
    数据类型方法装饰器
    :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args):

        self, k, v, instance = args
        try:
            self.key = k
            instance.datas[k] = self
            self.logger.info("process in method %s" % func.__name__)
            return format_response(b"200", b"success", func(*args))
        except (Empty, KeyError):
            if not self.data:
                del instance.datas[k]
            self.logger.error(traceback.format_exc())
            return format_response(b"502", b"Empty", b"")
        except Exception as e:
            self.logger.error(traceback.format_exc())
            return format_response(b"503", ("%s:%s"%(e.__class__.__name__.lower(), e)).encode("utf-8"), v)

    return wrapper


def common_cmd_wrapper(func):
    """
        通用方法装饰器
        :param func:
        :return:
        """
    @wraps(func)
    def wrapper(*args):

        self, k, v, instance = args
        try:
            return func(*args)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            return format_response(b"503", ("%s:%s"%(e.__class__.__name__.lower(), e)).encode("utf-8"), v)

    return wrapper


def format_response(code, info, data):
    if data is None:
        data = b""
    return b"%s#-*-#%s#-*-#%s\r\n\r\n" % (code, info, data)
