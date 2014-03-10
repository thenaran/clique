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


"""Application runtime information.
"""


import os
import pwd
from adt.config import JsonSettings
from adt.config import AppSettings
from adt.concurrency import Lazy


# Runtime context data
__DATA__ = Lazy()
__DATA__.data_dir = '''/data'''
__DATA__.app_dir = '''/app'''
__DATA__.home_dir = os.path.expanduser('~')
__DATA__.add_initializer('app_name',
                         lambda: pwd.getpwuid(os.geteuid()).pw_name)
__DATA__.add_initializer('context',
                         lambda: JsonSettings([CONTEXT_FILEPATH]))
__DATA__.add_initializer('config',
                         lambda: AppSettings([APP_CONFIG_PATH,
                                             os.path.join(__DATA__.home_dir,
                                                          APP_CONFIG_FILE_NAME)]))
__DATA__.add_initializer('info',
                         lambda: JsonSettings([APP_INFO_PATH]))
__DATA__.add_initializer('is_real',
                         lambda: os.path.exists(CONTEXT_FILEPATH))

# The context file to pass the engine context.
CONTEXT_FILEPATH = '''/data/.context'''
APP_INFO_FILE_NAME = '''.info'''
APP_INFO_PATH = os.path.join(__DATA__.app_dir, APP_INFO_FILE_NAME)
APP_CONFIG_FILE_NAME = '''app.config'''
APP_CONFIG_PATH = os.path.join(__DATA__.app_dir, APP_CONFIG_FILE_NAME)


def context():
  """The currently running engine's context data.
  """
  return __DATA__.context


def info():
  """Application specification data.
  """
  return __DATA__.info


def config():
  """Application configuration data.
  """
  return __DATA__.config


def app_name():
  """The currently running service app's string name.
  """
  return __DATA__.app_name


def app_version():
  """The currently running service app's string version.
  """
  return info().get('application.version')


def node_name():
  """The currently local node's string name.
  """
  return context().get('engine.name')


def node_version():
  """The currently local node's version
  """
  return context().get('engine.version')


def node_id():
  """The currently local node's identification
  """
  return context().get('engine.ident')


def node_updated_date():
  """The currently local node's updated date.
  """
  return context().get('engine.updated_date')


def data_dir():
  """Application data directory.
  """
  return __DATA__.data_dir


def app_dir():
  """Application app directory.
  """
  return __DATA__.app_dir


def home_dir():
  """Application home directory.
  """
  return __DATA__.home_dir


def is_real():
  """True if the application is running by the clique.
  """
  return __DATA__.is_real


def regiser_config_changed_handler(func):
  config().register_config_changed_handler(func)
