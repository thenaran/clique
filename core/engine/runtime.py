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


"""The engine runtime module.
"""

import logging
import os
import socket
from adt.config import JsonSettings
from adt.concurrency import Lazy
from adt.concurrency import CommandExecuter
import rpclib
import rpclib.server
import clique


__CONFIG_FILE_NAME__ = '''engine.config'''
__INFO_FILE_NAME__ = '''engine.info'''
__POLICY_FILE_NAME__ = '''engine.policy'''

# Runtime data
__DATA__ = Lazy()
__DATA__.add_initializer('context', lambda: Context())
__DATA__.add_initializer('engine_server', lambda: _init_engine_server())
__DATA__.add_initializer('policy', lambda: Policy())
__DATA__.add_initializer('task_queue', lambda: _init_worker())
__DATA__.executers = set()

# host name
HOST_NAME = socket.gethostname()
# clique user name
CLIQUE_USER = os.environ.get('CLIQUE_USER', 'clique')
# If root_path is defined, use it as a root directory.
ROOT_PATH = os.environ.get('ROOT_PATH', '/')
# engine code binaries
BASE_PATH = ROOT_PATH + '''core'''
# engien data directory
DATA_PATH = ROOT_PATH + '''data'''
# application base directory
APPS_PATH = ROOT_PATH + '''apps'''
# application home directory
HOME_PATH = os.path.expanduser('~%s' % CLIQUE_USER)
# store private key path
STORE_PRIVATE_KEY_PATH = ROOT_PATH + '''var/store/id_rsa'''
# store public key path
STORE_PUBLIC_KEY_PATH = ROOT_PATH + '''var/store/id_rsa.pub'''
# fundamental libraries for engine
FUNDAMENTALS_PATH = ROOT_PATH + '''fundamentals'''
# System log file path
LOG_PATH = ROOT_PATH + clique.LOG_PATH[1:]
# Main log filename
MAIN_LOG_FILENAME = '''clique'''
# Recovery related directory path
RECOVERY_PATH = HOME_PATH + '''/recovery'''
# App specification file path
APP_INFO_FILE_NAME = '''.info'''
# App settings file path
APP_CONFIG_FILE_NAME = '''app.config'''
# Rpc control port
RPC_CONTROL_PORT = 20001
# Node discover port
DISCOVER_PORT = 20002
# Task queue pool size
TASK_WORKERS_NUMBER = 10


class Context(object):
  """The engine runtime context.
  """
  def __init__(self, info_paths=None, setting_paths=None):
    # The engine expects to find the configuration file from the following
    # places:
    #   - the current user's home directory
    #   - under the /etc/clique directory
    #   - under the installation directory (where this module resides)
    paths = info_paths or [os.path.join(BASE_PATH, __INFO_FILE_NAME__)]
    self._engine_info = JsonSettings(paths)
    paths = setting_paths or [os.path.join(BASE_PATH, __CONFIG_FILE_NAME__),
                              os.path.join(HOME_PATH, __CONFIG_FILE_NAME__)]
    self._engine_settings = JsonSettings(paths)

  def rebase(self, path):
    """The rebase the given directory path to the engine base directory.
    """
    return os.path.join(BASE_PATH, os.path.isabs(path) and path[1:] or path)

  def rebase_to_data(self, path):
    """The rebase the given directory path to the data directory.
    """
    logging.debug("DATA_PATH for rebaes:%s", str(DATA_PATH))
    return os.path.join(DATA_PATH, os.path.isabs(path) and path[1:] or path)

  @property
  def engine_settings(self):
    return self._engine_settings

  @property
  def engine_info(self):
    return self._engine_info

  @property
  def engine_policy(self):
    return __DATA__.policy

  def get_app_settings(self, appname):
    """Gets the application settings.
    """
    base = self.get_app_base_dir(appname)
    home = self.get_app_home_dir(appname)
    if not os.path.exists(base):
      raise Exception("Application does not exist.")
    # The engine expects to find the app configuration file from the following
    # places:
    #   - the app user's home directory (chroot'ed)
    #   - under the /etc/_appbname_ directory (chroot'ed)
    #   - under the app installation directory (chroot'ed)
    paths = [
        # all app files are chroot'ed and reside under the app user's home dir.
        os.path.join(base, APP_CONFIG_FILE_NAME),
        os.path.join(home, APP_CONFIG_FILE_NAME),
    ]
    return JsonSettings(paths)

  def get_app_info(self, appname):
    """Gets the application settings.
    """
    base = self.get_app_base_dir(appname)
    if not os.path.exists(base):
      raise Exception("Application does not exist.")
    # The engine expects to find the app configuration file from the following
    # places:
    #   - the app user's home directory (chroot'ed)
    #   - under the /etc/_appbname_ directory (chroot'ed)
    #   - under the app installation directory (chroot'ed)
    paths = [
        # all app files are chroot'ed and reside under the app user's home dir.
        # When .info is not exists
        os.path.join(base, APP_INFO_FILE_NAME),
    ]
    return JsonSettings(paths)

  def get_app_root_dir(self, appname):
    """Gets the application root directory.
    """
    return os.path.join(APPS_PATH, appname)

  def get_app_base_dir(self, appname):
    """Gets the application base directory.
    """
    return os.path.join(self.get_app_root_dir(appname), 'app')

  def get_app_home_dir(self, appname):
    """Gets the application home directory.
    """
    return os.path.join(self.get_app_root_dir(appname), 'home', appname)

  def get_app_data_dir(self, appname):
    """Gets the application data directory.
    """
    return os.path.join(self.get_app_root_dir(appname), 'data')


def get_config_file_name():
  """Gets a config file name
  """
  return __CONFIG_FILE_NAME__


def context():
  """Gets the runtime engine context data.
  """
  return __DATA__.context


def _init_engine_server():
  """Engine server initializer.
  """
  import engine.identity
  server = rpclib.server.RpcServerOverTcp(port=RPC_CONTROL_PORT,
                                          allow_routing=True,
                                          ssl_options=engine.identity.get_server_ssl_options())
  port = server.serve()
  logging.info("Engine server initialized. port=%s", str(port))
  return server


def engine_server():
  """Engine control API server.
  """
  return __DATA__.engine_server


def _init_worker():
  """Background worker initializer.
  """
  from adt.concurrency import TaskQueue
  tq = TaskQueue(0, TASK_WORKERS_NUMBER)
  tq.start()
  return tq


def worker():
  """Add the callback as a background job.
  """
  return __DATA__.task_queue


def get_executer():
  """Gets a executer to run a commnad by Thread
  """
  return CommandExecuter(__DATA__.executers)


def register_aengel_message_handler(handler):
  """
    Handler example :

      def message_handler(msg):
        print msg
  """
  __DATA__.aengel_messenger.register_handler(handler)


def unregister_aengel_message_handler(handler):
  __DATA__.aengel_messenger.unregister_handler(handler)


def is_no_display():
  return os.path.exists(os.path.join(HOME_PATH, '.no_display'))


class Policy(object):
  """The engine policy data
  """

  # policy fields
  _privileged_key = '''privileged'''
  _immortals_key = '''reserved.immortals'''
  _namespaces_key = '''reserved.namespaces'''

  def __init__(self, paths=None):
    # The engine expects to find the configuration file from the following
    # places:
    #   - the current user's home directory
    #   - under the installation directory (where this module resides)
    self._policy = JsonSettings(paths or [
                                os.path.join(BASE_PATH,
                                             __POLICY_FILE_NAME__)
                                ])

  def is_immortal(self, appname):
    immortals = self._policy.get(self._immortals_key, [])
    return appname in immortals

  def is_reserved_namespace(self, namespace):
    namespaces = self._policy.get(self._namespaces_key, [])
    return namespace in namespaces

  def has_privileged(self, appname, fqn):
    key = '%s.%s' % (self._privileged_key, appname)
    if key in self._policy:
      modules = self._policy.get(key, [])
      return fqn in modules
    else:
      return False
