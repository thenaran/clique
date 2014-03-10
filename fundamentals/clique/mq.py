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

import logging
import random
import clique
import clique.isc
from rpclib.proxy import _RpcError, RpcError
from adt.concurrency import Future

__CALLBACKS__ = {}

# MQ ISC namespace
MQ_ISC_NAMESPACE = '''mq'''


def start():
  from clique.isc import register_endpoint
  register_endpoint(_handle_subscribe, namespace=MQ_ISC_NAMESPACE,
                    name='subscribe')
  register_endpoint(_handle_unsubscribe, namespace=MQ_ISC_NAMESPACE,
                    name='unsubscribe')


def stop():
  pass


def _handle_subscribe(topic, ep, expires=None):
  """Handle the subscribe request from others.
  """
  with clique.isc.context() as c:
    if ep.node_id and ep.node_id != c.node_id:
      logging.warn("Suspicious endpoint. Its node_id is different from where"
                   " it actuall came from. %s != %s", ep.node_id, c.node_id)
      raise Exception("Invalid node_id.")
    if ep.appname and ep.appname != c.appname:
      logging.warn("Suspicious endpoint. Its appname is different from where"
                   " it actuall came from. %s != %s", ep.appname, c.appname)
      raise Exception("Invalid appname.")
    ep.node_id = c.node_id
    ep.appname = c.appname
    try:
      # TODO: check if ep is valid.
      __CALLBACKS__[topic].append_ep(ep)
    except KeyError:
      raise TopicNotExistError("topic :%s for subscribe is not exist",
                               str(topic))


def _handle_unsubscribe(topic, ep):
  """ unsubscribe endpoint with topic name.
  """
  try:
    __CALLBACKS__[topic].remove_ep(ep)
  except KeyError:
    raise TopicNotExistError("topic :%s for unsubscribe is not exist",
                             str(topic))


def subscribe(topic, callback, expires=None):
  """Subscribe to the given *local* topic specified with topic name.
  """
  try:
    logging.debug("subscribe topic:%s, callback:%s",
                  str(topic), str(callback))
    __CALLBACKS__[topic].append_func(callback)
  except KeyError:
    raise TopicNotExistError("topic :%s for subscribe is not exist",
                             str(topic))


def unsubscribe(topic, callback):
  """unSubscribe to the given *local* topic specified with topic name.
  """
  try:
    __CALLBACKS__[topic].remove_func(callback)
  except KeyError:
    raise TopicNotExistError("topic :%s for unsubscribe is not exist",
                             str(topic))


def create_topic(topic, localonly=False):
  """Create a topic for distributing messages.
  """
  if topic not in __CALLBACKS__:
    __CALLBACKS__[topic] = Publisher(topic, localonly)
  else:
    raise Exception("creating topic :%s is already exists")


def delete_topic(topic):
  try:
    del __CALLBACKS__[topic]
  except KeyError:
    raise TopicNotExistError("deleting topic :%s is not exist")


def publish(topic, message):
  """Publish the message to all subscribers under the given topic.
  """
  try:
    __CALLBACKS__[topic](message)
  except KeyError:
    raise TopicNotExistError("topic to publish is not exists in mq. "
                             "topic: %s" % topic)


def queue(topic, message, expires=None):
  """Queue the message under the topic so that only one of the subscribers
  gets it (load-balancing). The subscriber could come later to get the message
  as long as it does not expire before then.
  """
  try:
    __CALLBACKS__[topic].queue(message)
  except KeyError:
    raise TopicNotExistError("topic to queue is not exists in mq")


class Publisher(object):
  def __init__(self, topic, localonly):
    self._callbacks = set()
    self._endpoints = {}
    self._topic = topic
    self._localonly = localonly

  def append_func(self, callback):
    self._callbacks.add(callback)

  def append_ep(self, ep):
    if self._localonly:
      raise Exception("Topic %s does not support remote callbacks" % self._topic)
    key = (ep.namespace, ep.name, ep.node_id,
           ep.appname)
    if key in self._endpoints:
      logging.debug("Endpoint already added. topic=%s, endpoint=%s",
                    self._topic, ep)
      return
    self._endpoints[key] = ep
    self._callbacks.add(ep)

  def remove_func(self, callback):
    self._callbacks.remove(callback)
    logging.debug("Removed a callback %s from topic %s", str(callback),
                  self._topic)

  def remove_ep(self, ep):
    try:
      del self._endpoints[(ep.namespace, ep.name,
                           ep.node_id, ep.appname)]
    except:
      logging.debug("Endpoint does not exist. Nothing to remove.",
                    exc_info=True)
    self._callbacks.remove(ep)

  def __call__(self, message):
    callbacks = self._callbacks
    for cb in callbacks:
      clique.do(self._do_callback, cb, self._topic, message)

  def queue(self, message):
    if len(self._callbacks) > 0:
      cb = random.choice(self._callbacks)
      clique.do(self._do_callback, cb, self._topic, message)

  def _do_callback(self, cb, topic, message):
    try:
      r = cb(topic, message)
      if isinstance(r, Future):
        r.then((lambda v: isinstance(v, Exception) and
                (hasattr(cb, 'node_id') and self.remove_ep(cb) or
                 self.remove_func(cb))))
    except:
      logging.warn("Error while invoking the callback in publisher. topic=%s",
                   topic, exc_info=True)


class _TopicNotExistError(_RpcError):
  """Raised the topic not exist (internal-only)
  """
  def convert(self):
    return TopicNotExistError(self.msg)


class TopicNotExistError(RpcError):
  """Raised the topic not exist
  """
  def convert(self):
    return _TopicNotExistError(self.message)
