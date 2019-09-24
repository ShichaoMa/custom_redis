# -*- coding:utf-8 -*-
import os
import re
import string

from configparser import ConfigParser
from contextlib import contextmanager
from setuptools import setup, find_packages


def get_version(package):
    """
    Return package version as listed in `__version__` in `__init__.py`.
    """
    init_py = open(os.path.join(package, '__init__.py')).read()
    mth = re.search("__version__\s?=\s?['\"]([^'\"]+)['\"]", init_py)
    if mth:
        return mth.group(1)
    else:
        raise RuntimeError("Cannot find version!")


def _compact_ver(name, ver):
    if ver == '"*"' or ver.startswith("{"):
        ver = ""
    return '%s%s' % (name, ver.strip('"'))


def install_requires(dev=False):
    """
    Return requires in requirements.txt
    :return:
    """
    try:
        cfg = ConfigParser()
        cfg.read('Pipfile')
        section_name = "%spackages" % ("dev-" if dev else "")
        requires = [_compact_ver(name, cfg.get(section_name, name))for name in cfg.options(section_name)]
        if not dev:
            with open("requirements.txt", "w") as f:
                f.write("\n".join(requires))
        return requires
    except OSError:
        return []

try:
    LONG_DESCRIPTION = open("README.md").read()
except UnicodeDecodeError:
    LONG_DESCRIPTION = open("README.md", encoding="utf-8").read()


@contextmanager
def cfg_manage(cfg_tpl_filename):
    if os.path.exists(cfg_tpl_filename):
        cfg_file_tpl = open(cfg_tpl_filename)
        buffer = cfg_file_tpl.read()
        try:
            with open(cfg_tpl_filename.rstrip(".tpl"), "w") as cfg_file:
                cfg_file.write(string.Template(buffer).substitute(
                    pwd=os.path.abspath(os.path.dirname(__file__))))
            yield
        finally:
            cfg_file_tpl.close()
    else:
        yield


with cfg_manage(__file__.replace(".py", ".cfg.tpl")):
    setup(
        name="custom-redis",
        version=get_version("custom_redis"),
        description="python实现简单redis，实现redis基本功能以及可插拔数据结构。 ",
        long_description=LONG_DESCRIPTION,
        classifiers=[
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Intended Audience :: Developers",
            "Operating System :: Unix",
        ],
        long_description_content_type="text/markdown",
        keywords="custom redis",
        author="cn",
        author_email="308299269@foxmail.com",
        url="https://www.github.com/ShichaoMa/custom_redis",
        entry_points={
            'console_scripts': [
                'custom-redis-server = custom_redis:start_server',
                'custom-redis-client = custom_redis:start_client',
        ],
        },
        license="MIT",
        packages=find_packages(exclude=("tests*",)),
        install_requires=install_requires(),
        include_package_data=True,
        zip_safe=True,
        setup_requires=["pytest-runner"],
        tests_require=install_requires(dev=True)
    )