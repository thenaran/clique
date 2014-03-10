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


"""The back module.
The back file structure
"""

import logging
import os
import tarfile
import engine.runtime
from engine.isc import endpoint
from engine.isc import PRIVILEGED
from engine.runtime import DATA_PATH, CLIQUE_USER
from engine.runtime import BASE_PATH, __CONFIG_FILE_NAME__
from engine.runtime import __POLICY_FILE_NAME__


__BASE_BACKUP_LIST__ = [os.path.join(DATA_PATH, '.auth'),
                        os.path.join(DATA_PATH, '.ident'),
                        os.path.join(DATA_PATH, 'nodes'),
                        os.path.join(BASE_PATH, __CONFIG_FILE_NAME__),
                        os.path.expanduser('~/%s/%s' % (CLIQUE_USER,
                                                        __CONFIG_FILE_NAME__)),
                        os.path.expanduser('~/%s' % __POLICY_FILE_NAME__),
                        '/etc/clique/%s' % __POLICY_FILE_NAME__,
                        os.path.join(BASE_PATH, __POLICY_FILE_NAME__)]

__BASE_BACKUP_FILE_NAME__ = '''.base'''


def validate():
  pass


def start():
  pass


def stop():
  pass


def _get_app_backup_list(appname):
  return [engine.runtime.context().get_app_data_dir(appname),
          engine.runtime.context().get_app_home_dir(appname)]


@endpoint(protection=PRIVILEGED)
def backup(path, appname=None):
  """ start backup
  if appname is none, backup the base
  """
  logging.debug("backup path:%s, appname:%s", str(path), str(appname))
  if not os.path.exists(path):
    return Exception("backup path:%s not exists", str(path))

  backup_path = _get_backup_path(path, appname)

  try:
    backup_list = _get_app_backup_list(appname) if appname else __BASE_BACKUP_LIST__
    logging.debug("backup list:%s", str(backup_list))

    with tarfile.open(backup_path, 'w:bz2') as tar:
      for f in backup_list:
        tar.add(f)

    return backup_path
  except Exception, e:
    logging.debug("Fail to backup path:%s, appname:%s to backup_path:%s",
                  str(path), str(appname), str(backup_path))
    return e


@endpoint(protection=PRIVILEGED)
def restore(path, appname=None):
  """ restore backup data in given path,
  if app version of path and current app version is not matched,
  the restore data of application may be not worked on current application.
  so should check uncompatible app and warn about that before restore
  """
  logging.debug("restore path:%s, appname:%s", str(path), str(appname))
  backup_path = _get_backup_path(path, appname)
  if not os.path.exists(backup_path):
    return Exception("backup path:%s not exists", str(path))

  try:
    with tarfile.open(backup_path, 'r:bz2') as tar:
      tar.extractall('/')
    return backup_path
  except Exception, e:
    logging.debug("Fail to restore path:%s, appname:%s to backup path:%s",
                  str(path), str(appname), str(backup_path))
    return e


def _get_backup_path(path, appname):
  if appname:
    return os.path.join(path, appname)
  else:
    return os.path.join(path, __BASE_BACKUP_FILE_NAME__)
