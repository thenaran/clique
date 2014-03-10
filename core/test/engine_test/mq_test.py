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


"""Test cases for mq.
"""
import os
from nose.plugins.attrib import attr

from rpclib.testing import RpcTestCase
from rpclib.stackcontext import wrap
from adt.funcs import try_execute
from engine.runtime import Context
from engine.isc import endpoint
import engine.mq
from clique.mq import TopicNotExistError
from clique.mq import MQ_ISC_NAMESPACE
import engine
import engine.isc
import engine.runtime
import clique.runtime
from adt.config import JsonSettings


@attr(species="clique", genus="core", family="engine", name="mq")
class MQTestCase(RpcTestCase):
  def setUp(self):
    RpcTestCase.setUp(self)
    engine.first.generate_rsa_keys()
    self.old_call = clique.isc.Endpoint.__call__
    self._old_add_service_route = engine.isc.add_service_route
    engine.isc.add_service_route = lambda appname: None
    clique.isc.Endpoint.__call__ = engine.isc.__call__
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'test.config')
    info_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'test.info')
    engine.runtime.__DATA__.context = Context([info_path], [config_path])
    clique.runtime.__DATA__.context = JsonSettings([config_path])
    engine.isc.start()
    engine.mq.start()
    #don't need to restore

  def tearDown(self):
    RpcTestCase.tearDown(self)
    engine.isc.stop()
    engine.mq.stop()
    engine.runtime.engine_server().stop()
    del engine.runtime.__DATA__.engine_server
    del engine.runtime.__DATA__.context
    del clique.runtime.__DATA__.context
    del engine.identity.__DATA__.ident_db_path
    del engine.identity.__DATA__.ident_db
    del engine.node.__DATA__.last_accessed_times
    engine.isc.add_service_route = self._old_add_service_route
    clique.isc.Endpoint.__call__ = self.old_call
    try_execute(os.remove, engine.identity.__DATA__.ident_db_path)
    try_execute(os.remove, engine.runtime.context().rebase_to_data('.auth'))

  def test_subscribe(self):
    """test subscribe, call subscribe using isc
    """
    topic = 'topic'
    engine.mq.create_topic(topic)

    callback_namespace = 'namespace'
    callback_name = 'name'
    message = '11111'

    @endpoint(callback_namespace, callback_name)
    def func(t, m):
      self.assertEqual(t, topic)
      self.assertEqual(m, message)
      self.stop()

    def do_check(v):
      self.assertTrue(topic in clique.mq.__CALLBACKS__)
      clique.mq.__CALLBACKS__[topic](message)

    def do_test():
      callback_ep = engine.isc.find({'namespace': callback_namespace,
                                     'name': callback_name})[0]
      ep = clique.isc.Endpoint(MQ_ISC_NAMESPACE, 'subscribe',
                               node_id=engine.node.ident())
      ep(topic, callback_ep).then(do_check)

    with open(engine.identity.get_ssl_options()['certfile']) as f:
      engine.identity.register(engine.node.ident(), f.read())
    self.ioloop().add_callback(wrap(do_test))
    self.start()

  def test_publish(self):
    """test publish, call publish using isc
    """
    topic = 'topic'
    engine.mq.create_topic(topic)
    message = 'message'

    callback_namespace = 'namespace'
    callback_name = 'name'

    @endpoint(callback_namespace, callback_name)
    def func(t, m):
      self.assertEqual(t, topic)
      self.assertEqual(m, message)
      self.stop()

    def do_test():
      callback_ep = engine.isc.find({'namespace': callback_namespace,
                                     'name': callback_name})[0]
      clique.mq.__CALLBACKS__[topic].append_ep(callback_ep)
      clique.mq.publish(topic, message)

    with open(engine.identity.get_ssl_options()['certfile']) as f:
      engine.identity.register(engine.node.ident(), f.read())
    self.ioloop().add_callback(wrap(do_test))
    self.start()

  def test_topic_not_exist(self):
    """test if it fails when topic does not exist.
    """
    def response(err):
      self.fail("Must not get here.")

    def do_test():
      with self.assertRaises(TopicNotExistError):
        clique.mq.subscribe('notexist', response)
      self.stop()

    self.ioloop().add_callback(wrap(do_test))
    self.start()
