# -*- coding:utf-8 -*-
import codecs
import os
try:
    from setuptools import setup, find_packages
except:
    from distutils.core import setup

from custom_redis import AUTHOR, AUTHOR_EMAIL, VERSION, URL, NAME, DESCRIPTION


def read(fname):
    return codecs.open(os.path.join(os.path.dirname(__file__), fname)).read()

LONG_DESCRIPTION = read("README.rst")

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
    install_requires=["multi-thread-closing"],
    include_package_data=True,
    zip_safe=True,
)