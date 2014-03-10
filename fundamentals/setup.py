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


import sys
import os
from glob import glob
from setuptools import setup
from distutils.core import Extension
from distutils.command.sdist import sdist
from distutils.command.build_ext import build_ext
import atexit


# Change the current working directory to where this setup.py script is.
if os.getcwd() != os.path.dirname(os.path.abspath(__file__)):
  cwd = os.getcwd()
  atexit.register(lambda: os.chdir(cwd))
  os.chdir(os.path.dirname(__file__))


try:
  import Cython.Compiler.Main as cython_compiler
  have_cython = True
except ImportError:
  have_cython = False


def cythonize(src):
  sys.stderr.write("cythonize: %r\n" % (src,))
  cython_compiler.compile([src])


def ensure_source(src):
  sys.stderr.write("@@ensure.source:%s\n" % src)
  pyx = os.path.splitext(src)[0] + '.pyx'

  if not os.path.exists(src):
    if not have_cython:
      raise Exception("""\
          Cython is required for building extension from checkout.
          Install Cython >=0.16 or install msgpack from PyPI.
          """)
    cythonize(pyx)
  elif (os.path.exists(pyx) and
        os.stat(src).st_mtime < os.stat(pyx).st_mtime and
        have_cython):
    cythonize(pyx)
  return src


class BuildExt(build_ext):
  def build_extension(self, ext):
    sys.stderr.write("@@build extension the sources:%s\n" % str(ext.sources))
    ext.sources = list(map(ensure_source, ext.sources))
    return build_ext.build_extension(self, ext)

kwargs = {}

extensions = []
major, minor = sys.version_info[:2]


def get_version():
  path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'clique',
                                      '__init__.py'))
  version = os.environ.get('VERSION', 'unknown')
  with open(path, 'r') as r:
    lines = r.readlines()
  with open(path, 'w') as w:
    for i in range(len(lines)):
      if lines[i].startswith('VERSION'):
        w.write('VERSION = "%s"\n' % version)
        w.writelines(lines[i + 1:])
        break
      else:
        w.write(lines[i])
  return version


def get_readme():
  path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'README.mkd')
  with open(path, 'r') as f:
    return f.read()


def get_license():
  path = os.path.join(
      os.path.dirname(os.path.abspath(__file__)), 'LICENSE.mkd')
  with open(path, 'r') as f:
    return f.read()


version = get_version()
readme = get_readme()
license = get_license()

if sys.byteorder == 'big':
    macros = [('__BIG_ENDIAN__', '1')]
else:
    macros = [('__LITTLE_ENDIAN__', '1')]

if major >= 3:
  kwargs["use_2to3"] = True

if have_cython:
  class Sdist(sdist):
    def __init__(self, *args, **kwargs):
      for src in glob('rpclib/msgpack/*.pyx'):
        sys.stderr.write("@@Sdist source:%s\n" % str(src))
        cythonize(src)
      sdist.__init__(self, *args, **kwargs)
else:
  Sdist = sdist

extensions.append(Extension('rpclib.msgpack._msgpack',
                            sources=['rpclib/msgpack/_msgpack.c'],
                            include_dirs=['.'],
                            define_macros=macros
                            ))
extensions.append(Extension('adt.llist',
                            sources=['adt/llist/llist.c',
                                     'adt/llist/dllist.c',
                                     'adt/llist/sllist.c']
                            ))
extensions.append(Extension('hal._wiringpi',
                            sources=['hal/WiringPi/wiringPi/lcd.c',
                                     'hal/WiringPi/wiringPi/piHiPri.c',
                                     'hal/WiringPi/wiringPi/piThread.c',
                                     'hal/WiringPi/wiringPi/wiringPiFace.c',
                                     'hal/wiringpi_wrap.c',
                                     'hal/WiringPi/wiringPi/wiringPi.c',
                                     'hal/WiringPi/wiringPi/wiringSerial.c',
                                     'hal/WiringPi/wiringPi/wiringShift.c',
                                     'hal/WiringPi/wiringPi/wiringPiSPI.c',
                                     'hal/WiringPi/wiringPi/softPwm.c',
                                     'hal/WiringPi/wiringPi/piNes.c',
                                     'hal/WiringPi/wiringPi/gertboard.c']
                            ,
                            include_dirs=['hal/WiringPi/wiringPi']))



python_26 = (major > 2 or (major == 2 and minor >= 6))
if "linux" in sys.platform.lower() and not python_26:
  extensions.append(Extension(
                    "rpclib.tornado.epoll", ["rpclib/tornado/epoll.c"]))


del macros

setup(
    name="clique-fundamentals",
    version=version,
    cmdclass={'build_ext': BuildExt, 'sdist': Sdist},
    packages=[
        "adt", "adt.llist",
        "clique",
        "hal",
        "rpclib", "rpclib.tornado", "rpclib.tornado.platform", "rpclib.msgpack",
    ],
    ext_modules=extensions,
    author="Narantech Inc.",
    author_email="clique@narantech.com",
    url="http://clique.to/",
    license=license,
    description="Clique is a distributed application platform designed to scale"
                "from a single computer to thousands of computational nodes.",
    long_description=readme,
    **kwargs
)
