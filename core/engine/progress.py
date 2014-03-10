# -*- coding: utf-8 -*-
# Copyright 2012-2013 Narantech Inc. All rights reserved.
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|


"""Progress management.
"""


# Clique related libraries
import engine.mq
from engine.isc import endpoint
from adt.concurrency import Lazy
from rpclib.proxy import Base

# Progress Topic
PROGRESS_UPDATED_TOPIC = '''progress.updated'''


__DATA__ = Lazy()
__DATA__.progresses = {}


class ProgressInfo(Base):
  def __init__(self, ident, current, total, response):
    self.ident = ident
    self.current = current
    self.total = total
    self.response = response


def validate():
  pass


def start():
  """
  Note:
    Edits comment of start function in main.py, if edit it.
  """
  engine.mq.create_topic(PROGRESS_UPDATED_TOPIC)


def stop():
  engine.mq.delete_topic(PROGRESS_UPDATED_TOPIC)


def build_ident(name, app=None):
  if app:
    return '%s_%s' % (name, app)
  return '%s' % name


def update_progress(ident, now=0, total=100, response=None):
  __DATA__.progresses[ident] = (now, total, response)
  engine.mq.publish(PROGRESS_UPDATED_TOPIC,
                    ProgressInfo(ident, now, total, response))


def get_idents():
  return __DATA__.progresses.keys()


@endpoint()
def get_progresses():
  return __DATA__.progresses


@endpoint()
def get_progress(ident):
  return __DATA__.progresses[ident]
