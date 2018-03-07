# -*- coding:utf-8 -*-
try:
    from setuptools import setup, find_packages
except:
    from distutils.core import setup


VERSION = '3.1.3'

AUTHOR = "cn"

AUTHOR_EMAIL = "308299269@foxmail.com"

URL = "https://www.github.com/ShichaoMa/custom_redis"

NAME = "custom-redis"

DESCRIPTION = "python实现简单redis，实现redis基本功能以及可插拔数据结构。 "

try:
    LONG_DESCRIPTION = open("README.rst").read()
except UnicodeDecodeError:
    LONG_DESCRIPTION = open("README.rst", encoding="utf-8").read()


KEYWORDS = "custom redis"

LICENSE = "MIT"

PACKAGES = ["custom_redis", "custom_redis.client", "custom_redis.server"]

setup(
    name = NAME,
    version = VERSION,
    description = DESCRIPTION,
    long_description = LONG_DESCRIPTION,
    classifiers = [
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
    ],
    entry_points={
        'console_scripts': [
            'custom-redis-server = custom_redis:start_server',
            'custom-redis-client = custom_redis:start_client',
        ],
    },
    keywords = KEYWORDS,
    author = AUTHOR,
    author_email = AUTHOR_EMAIL,
    url = URL,
    license = LICENSE,
    packages = PACKAGES,
    install_requires=["toolkit"],
    include_package_data=True,
    zip_safe=True,
)