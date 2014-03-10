#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2012-2013 Narantech Inc. All rights reserved.
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|


import os
import sys
from setuptools import setup
import atexit


# Change the current working directory to where this setup.py script is.
if os.getcwd() != os.path.dirname(os.path.abspath(__file__)):
  cwd = os.getcwd()
  atexit.register(lambda: os.chdir(cwd))
  os.chdir(os.path.dirname(__file__))


kwargs = {}
extensions = []
major, minor = sys.version_info[:2]


def get_version():
  version = os.environ.get('VERSION', 'unknown')
  import subprocess
  path = os.path.join(
      os.path.dirname(os.path.abspath(__file__)), 'engine', '__init__.py')
  cmd = 'sed -i "s/^VERSION =.*/VERSION = \'%s\'/g" %s' % (version, path)
  subprocess.check_call(cmd, shell=True)
  path = os.path.join(
      os.path.dirname(os.path.abspath(__file__)), 'engine.config')
  cmd = 'sed -i "s/^version=.*/version=%s/g" %s' % (version, path)
  subprocess.check_call(cmd, shell=True)
  return version


def get_readme():
  path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.mkd")
  with open(path, 'r') as f:
    return f.read()


def get_license():
  path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'LICENSE')
  with open(path, 'r') as f:
    return f.read()


version = get_version()
readme = get_readme()
license = get_license()


if major >= 3:
  kwargs["use_2to3"] = True


setup(
    name="clique-engine",
    version=version,
    packages=[
        "engine", "engine/ifcfg", "engine/wifi"],
    package_data={},
    ext_modules=extensions,
    author="Naran Inc.",
    author_email="clique@thenaran.com",
    url="http://thenaran.com",
    license=license,
    description="Clique_engine is host core engine.",
    long_description=readme,
    **kwargs
)
