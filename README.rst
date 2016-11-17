custom_redis
============
DESCRIPTION
-----------
- python实现简单redis，实现redis基本功能以及可插拔数据结构
    -主要功能
        1. 通过继承DataStore类，可以定制个性化数据类型，通过调用redis类的install方法安装数据类型，目前已实现的数据类型有str, set, queue, hash, list
        #. Redis 的keys, expire, ttl, del等功能已实现
        #. 数据持久化功能已实现, 数据库文件保存在当前目录

INSTALL
-------
windows && ubuntu
>>>>>>>>>>>>>>>>>
::

    git clone https://github.com/ShichaoMa/custom_redis.git
    sudo python setup.py install

    or

    sudo pip install custom-redis

START
>>>>>
::

    custom-redis-server --host "127.0.0.1" --port 6379

HELLOWORD
>>>>>>>>>
- demo1
::

    custom-redis-client -c keys
    [u'a']
    custom-redis-client -c zcard a
    1
    custom-redis-client -c zpop a
    2

- demo2
::

    >>> from custom_redis.client import Redis
    >>> r = Redis("127.0.0.1", 6379)
    >>> r.zadd('a', 3, 'aaa')
    ''
    >>> r.zadd('b', 4, 'bbb')
    ''
    >>> r.zadd('a', 1, 'ccc')
    ''
    >>> r.keys()
    [u'a', u'b']
    >>> r.expire('b', 10)
    ''
    >>> import time
    >>> time.sleep(10)
    >>> r.ttl('b')
    '-1'
    >>> r.keys()
    [u'a']

- demo3

    - 服务端实现
    - 数据类型个性化定制
    - 参见default_data_types.py
    - 安装数据类型
    ::

        cr = CustomRedis.parse_args()
        cr.install(datatype=datatype())
        cr.set_logger()
        cr.start()

- demo4

    - 客户端实现
    - 在functions.CMD_DICT中配置指令
    ::

        "hgetall":  # 指令名
        {
        "args": ["name"], # 指令所需参数
        "recv": lambda data: json.loads(data), # 转换函数，将接收到的报文转换成所需数据类型，若是简单字符串则无需提供
        }
        "hincrby": {
        "args": ["name", "key", "value"],
        "send": lambda *args: (args[0], json.dumps(dict([args[1:]]))), # 转换函数，返回key 和value组成的元组
        "default":[1] # 指令所需参数的默认值， 从后往前排列 如：1代表value的值
        },

    - 发送报文的格式："func_name#-*-#key<->value#-*-#1" # 如 "hincrby#-*-#a<->{'b':1}#-*-#1"
    - 最后一位1代表keep-alive, 否则为空
    - key:在redis中存储的key，在上例中所指的是参数中的name
    - value:要往redis中存储的值 如json.dumps("key":"value")
    - 接收报文格式："200#-*-#success#-*-#data"
    - 200为响应码
        -  200：成功
        -  404：没有找到方法
        -  502：Empty
        -  503：服务器异常
    - success为响应信息
    - data为recv中要处理的接收数据

