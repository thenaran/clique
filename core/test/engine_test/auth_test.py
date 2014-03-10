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

""" Test cases for authontication
"""
import os
import logging

from nose.plugins.attrib import attr
from rpclib.testing import RpcTestCase
from engine.runtime import Context
from engine.isc import endpoint
from adt.config import JsonSettings
from rpclib.stackcontext import wrap
from rpclib.proxy import SingleRpcProxy
from clique.isc import InvalidNodeNameError, IdentityNotFoundError
from clique.isc import AccessDeniedError
from clique.isc import UnauthorizedError

import engine.first
import engine.runtime
import engine.node
import clique.runtime
import clique.isc


@attr(species="clique", genus="core", family="engine", name="auth")
class AuthTestCase(RpcTestCase):
  """ Test for auth
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    del engine.node.__DATA__.node_id
    engine.first.generate_rsa_keys()
    self._old_add_service_route = engine.isc.add_service_route
    engine.isc.add_service_route = lambda appname: None
    self.old_get_unix_socket_in_path = engine.isc.get_unix_socket_in_path
    self.old_execute_cmd = engine.aengel.execute_cmd
    self.old_call = clique.isc.Endpoint.__call__
    engine.isc.get_unix_socket_in_path = \
        lambda a: clique.isc.INTERNAL_SOCKET_IN_PATH
    clique.isc.Endpoint.__call__ = engine.isc.__call__
    engine.aengel.execute_cmd = lambda c: None
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'test.config')
    info_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'test.info')
    engine.runtime.__DATA__.context = Context([info_path], [config_path])
    clique.runtime.__DATA__.context = JsonSettings([config_path])
    engine.isc.start()
    #register pubkey
    with open(engine.identity.get_ssl_options()['certfile']) as f:
      engine.identity.register(engine.node.ident(), f.read())

  def tearDown(self):
    RpcTestCase.tearDown(self)
    engine.isc.stop()
    engine.runtime.engine_server().stop()
    del engine.runtime.__DATA__.engine_server
    del engine.runtime.__DATA__.context
    del clique.runtime.__DATA__.context
    del engine.node.__DATA__.nodename
    del engine.node.__DATA__.node_id
    del engine.node.__DATA__.last_accessed_times
    del engine.identity.__DATA__.ident_db_path
    del engine.identity.__DATA__.ident_db
    engine.isc.add_service_route = self._old_add_service_route
    engine.isc.get_unix_socket_in_path = self.old_get_unix_socket_in_path
    engine.aengel.execute_cmd = self.old_execute_cmd
    clique.isc.Endpoint.__call__ = self.old_call
    if os.path.exists(engine.identity.__DATA__.ident_db_path):
      os.remove(engine.identity.__DATA__.ident_db_path)
    if os.path.exists(engine.runtime.context().rebase_to_data('.auth')):
      os.remove(engine.runtime.context().rebase_to_data('.auth'))

  def test_node_name_invalid_exception(self):
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = "return"
    query = {'namespace': namespace, 'name': name}

    @endpoint(namespace, name)
    def func(a1, a2):
      return func_return

    @wrap
    def callback(result):
      logging.debug("result:%s", str(result))
      self.assertTrue(isinstance(result, InvalidNodeNameError))
      self.stop()

    def post_condition():
      # get eps.
      eps = engine.isc.find(query)
      future = eps[0](arg1, arg2)
      future.get_value(callback)

    engine.node.__DATA__.node_id = 'unknown'

    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_ident_not_found_exception(self):
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = "return"
    query = {'namespace': namespace, 'name': name}

    @endpoint(namespace, name)
    def func(a1, a2):
      return func_return

    @wrap
    def callback(result):
      self.assertTrue(isinstance(result, IdentityNotFoundError))
      self.stop()

    def post_condition():
      # get eps.
      eps = engine.isc.find(query)
      future = eps[0](arg1, arg2)
      future.get_value(callback)

    engine.identity.unregister(engine.node.ident())
    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_access_denied_on_privileged(self):
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = "return"
    query = {'namespace': namespace, 'name': name}

    @endpoint(namespace, name, protection=clique.isc.PRIVILEGED)
    def func(a1, a2):
      return func_return

    @wrap
    def callback(result):
      self.assertTrue(isinstance(result, AccessDeniedError))
      self.stop()

    def post_condition():
      # get eps.
      eps = engine.isc.find(query)
      future = eps[0](arg1, arg2)
      future.get_value(callback)

    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_access_denied_on_nodeonly(self):
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = "return"
    query = {'namespace': namespace, 'name': name}

    @endpoint(namespace, name, protection=clique.isc.NODEONLY)
    def func(a1, a2):
      return func_return

    @wrap
    def callback(result):
      logging.debug("result:%s", str(result))
      self.assertTrue(isinstance(result, AccessDeniedError))
      self.stop()

    def post_condition():
      # get eps.
      eps = engine.isc.find(query)
      future = eps[0](arg1, arg2)
      future.get_value(callback)

    def call(self, *args, **kwargs):
      # RpcProxy takes care of the routing this request to the appropriate
      # service application by looking at the appname as its destination ID.
      if self.node_id == engine.node.ident() and self.appname:
        address = engine.isc.get_unix_socket_in_path(self.appname)
      else:
        address = engine.node.get_address(self.node_id)

      proxy = SingleRpcProxy(address, clique.isc.ISC_RPC_NAMESPACE,
                             clique.isc.build_location(engine.node.ident(), 'appname'),
                             clique.isc.build_location(self.node_id, self.appname),
                             timeout=self.timeout,
                             ssl_options=engine.identity.get_ssl_options())
      return proxy.handle(self.namespace, self.name, *args, **kwargs)

    clique.isc.Endpoint.__call__ = call

    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_normal_handle_in_privileged(self):
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = "return"
    query = {'namespace': namespace, 'name': name}

    @endpoint(namespace, name, protection=clique.isc.NODEONLY)
    def func(a1, a2):
      self.assertEqual(a1, arg1)
      self.assertEqual(a2, arg2)
      return func_return

    @wrap
    def callback(result):
      self.assertEqual(result, func_return)
      self.stop()

    def post_condition():
      # get eps.
      eps = engine.isc.find(query)
      future = eps[0](arg1, arg2)
      future.get_value(callback)

    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_unauthonticated_exception(self):
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = "return"
    query = {'namespace': namespace, 'name': name}

    @endpoint(namespace, name, elevated=True)
    def func(a1, a2):
      return func_return

    @wrap
    def callback(result):
      self.assertTrue((engine.node.ident() + ".", namespace, name) in engine.isc.__DATA__.auth_pending_list)
      self.assertTrue(isinstance(result, UnauthorizedError))
      self.stop()

    def post_condition():
      # get eps.
      eps = engine.isc.find(query)
      future = eps[0](arg1, arg2)
      future.get_value(callback)

    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_rerequest_after_allow(self):
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    karg1 = 'karg1'
    query = {'namespace': namespace, 'name': name}

    @endpoint(namespace, name, elevated=True)
    def func(a1, a2, karg=None):
      self.assertEqual(a1, arg1)
      self.assertEqual(a2, arg2)
      self.assertEqual(karg, karg1)
      self.stop()

    @wrap
    def callback(result):
      self.assertTrue((engine.node.ident() + ".", namespace, name) in engine.isc.__DATA__.auth_pending_list)
      self.assertTrue(isinstance(result, UnauthorizedError))
      engine.isc.allow(engine.node.ident() + ".", namespace, name, 10)

    def post_condition():
      # get eps.
      eps = engine.isc.find(query)
      future = eps[0](arg1, arg2, karg=karg1)
      future.get_value(callback)

    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_allow_request(self):
    source = 'source'
    namespace = 'namespace'
    name = 'name'

    engine.isc.allow(source, namespace, name, 10)

    self.assertTrue((source, namespace, name) in engine.isc.__DATA__.allowed_sources)
    auths = engine.isc.dump_auth_info()
    self.assertEqual(len(auths), 1)
    self.assertEqual(auths[0].source, source)
    self.assertEqual(auths[0].namespace, namespace)
    self.assertEqual(auths[0].name, name)

  def test_invalidate(self):
    pass

  def test_get_pending_auth_infos(self):
    pass

  def test_get_allowed_auth_infos(self):
    pass
