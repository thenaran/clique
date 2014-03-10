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


"""Message-Queue module.
"""

import clique.mq


MQ_ISC_NAMESPACE = clique.mq.MQ_ISC_NAMESPACE


def validate():
  pass


def start():
  """
  Note:
    Edits comment of start function in main.py, if edit it.
  """
  from engine.isc import register_engine_endpoint
  register_engine_endpoint(clique.mq._handle_subscribe,
                           namespace=MQ_ISC_NAMESPACE,
                           name='subscribe',
                           description="subscribe event message related to "
                                       "topic in engine")


def stop():
  clique.mq.__CALLBACKS__.clear()


def subscribe(*args, **kwargs):
  return clique.mq.subscribe(*args, **kwargs)


def create_topic(*args, **kwargs):
  return clique.mq.create_topic(*args, **kwargs)


def delete_topic(*args, **kwargs):
  return clique.mq.delete_topic(*args, **kwargs)


def publish(*args, **kwargs):
  return clique.mq.publish(*args, **kwargs)


def queue(*args, **kwargs):
  return clique.mq.queue(*args, **kwargs)
