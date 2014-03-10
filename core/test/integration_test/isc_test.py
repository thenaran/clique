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

"""isc integration testing.
"""
import os
from threading import Thread
from nose.plugins.attrib import attr

from engine.runtime import Context
from adt.config import JsonSettings
from adt.funcs import try_execute
from rpclib.testing import RpcTestCase
from rpclib.proxy import InvalidDestinationError, DestinationNotFoundError
from rpclib.stackcontext import wrap
from rpclib.proxy import SingleRpcProxy
from clique.isc import Endpoint
from clique.isc import build_location
import clique.isc
import engine.isc
import clique.runtime
import engine.runtime
import engine.node


@attr(species="clique", genus="core", family="integration", name="isc")
class IscIntegrationTestCase(RpcTestCase):
  def setUp(self):
    RpcTestCase.setUp(self)
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'test.config')
    info_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'test.info')
    engine.runtime.__DATA__.context = Context([info_path], [config_path])
    clique.runtime.__DATA__.context = JsonSettings([config_path])
    self.old_get_unix_socket_out_path = engine.isc.get_unix_socket_out_path
    engine.isc.get_unix_socket_out_path = \
        lambda a: clique.isc.INTERNAL_SOCKET_OUT_PATH
    self.old_get_unix_socket_in_path = engine.isc.get_unix_socket_in_path
    engine.isc.get_unix_socket_in_path = \
        lambda a: clique.isc.INTERNAL_SOCKET_IN_PATH
    self.old_internal_socket_in_path = clique.isc.INTERNAL_SOCKET_IN_PATH
    self.old_internal_socket_out_path = clique.isc.INTERNAL_SOCKET_OUT_PATH
    self.old_execute_cmd = engine.aengel.execute_cmd
    self.old_build_location = clique.isc.build_location
    engine.aengel.execute_cmd = lambda c: None
    clique.isc.INTERNAL_SOCKET_IN_PATH = '''/tmp/_socket_in'''
    clique.isc.INTERNAL_SOCKET_OUT_PATH = '''/tmp/_socket_out'''
    clique.runtime.__DATA__.is_real = True
    clique.runtime.__DATA__.app_name = "appname"
    engine.isc.start()
    clique.isc.start()
    engine.isc.add_service_route(clique.runtime.app_name())

  def tearDown(self):
    RpcTestCase.tearDown(self)
    engine.isc.remove_service_route(clique.runtime.app_name())
    engine.isc.stop()
    clique.isc.stop()
    engine.runtime.engine_server().stop()
    del engine.runtime.__DATA__.engine_server
    del engine.runtime.__DATA__.context
    del clique.runtime.__DATA__.context
    del clique.runtime.__DATA__.is_real
    del clique.runtime.__DATA__.app_name
    del engine.identity.__DATA__.ident_db_path
    del engine.identity.__DATA__.ident_db
    engine.isc.get_unix_socket_out_path = self.old_get_unix_socket_out_path
    engine.isc.get_unix_socket_in_path = self.old_get_unix_socket_in_path
    clique.isc.INTERNAL_SOCKET_IN_PATH = self.old_internal_socket_in_path
    clique.isc.INTERNAL_SOCKET_OUT_PATH = self.old_internal_socket_out_path
    clique.isc.build_location = self.old_build_location
    engine.aengel.execute_cmd = self.old_execute_cmd
    try_execute(os.remove, engine.identity.__DATA__.ident_db_path)
    try_execute(os.remove, engine.runtime.context().rebase_to_data('.auth'))

  def test_routing_app_to_same_node_app_without_nodename(self):
    """ test routing A -> N -> A
    request endpoint doesn't has node name but has only app name
    this case is that request by directly created endpoint
    """
    namespace = 'namespcae'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = 'return'

    @wrap
    def callback(result):
      self.assertEqual(result, func_return)
      self.stop()

    def func(a1, a2):
      self.assertEqual(arg1, a1)
      self.assertEqual(arg2, a2)
      return func_return

    @wrap
    def response(result):
      ep = Endpoint(namespace, name, '', clique.runtime.app_name())
      future = ep(arg1, arg2)
      future.get_value(callback)

    f = clique.isc.register_endpoint(func, namespace, name)
    f.get_value(response)
    self.start()

  def test_routing_app_to_same_node_app_with_same_nodename(self):
    """ test routing A -> N -> A
    request endpoint has same node name and other app name
    this case is that request by registered endpoint from other app
    """
    namespace = 'namespcae'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = 'return'

    @wrap
    def callback(result):
      self.assertEqual(result, func_return)
      self.stop()

    def func(a1, a2):
      self.assertEqual(arg1, a1)
      self.assertEqual(arg2, a2)
      return func_return

    @wrap
    def response(result):
      ep = Endpoint(namespace, name, engine.node.ident(),
                    clique.runtime.app_name())
      future = ep(arg1, arg2)
      future.get_value(callback)

    f = clique.isc.register_endpoint(func, namespace, name)
    f.get_value(response)
    self.start()

  def test_routing_app_to_same_node(self):
    """ test routing A -> N
    request endpoint has same node name and doesn't has app name
    this case is that request from same node app to same node
    """

    namespace = 'namespcae'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = 'return'

    @wrap
    def callback(result):
      self.assertEqual(result, func_return)
      self.stop()

    def func(a1, a2):
      self.assertEqual(arg1, a1)
      self.assertEqual(arg2, a2)
      return func_return

    @wrap
    def response():
      ep = Endpoint(namespace, name, engine.node.ident())
      future = ep(arg1, arg2)
      future.get_value(callback)

    engine.isc.register_engine_endpoint(func, namespace, name)
    self.ioloop().add_callback(response)
    self.start()

  def test_routing_app_to_other_node(self):
    """ test routing A -> N -> N'
    """
    namespace = 'namespcae'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = 'return'
    src = 'src'

    address = engine.node.__DATA__.localhost
    source = build_location(src)
    destination = build_location(engine.node.ident())
    isc_rpc_namespace = clique.isc.ISC_RPC_NAMESPACE
    timeout = 10

    @wrap
    def callback(result):
      self.assertEqual(result, func_return)
      self.stop()

    def func(a1, a2):
      with clique.isc.context() as c:
        self.assertEqual(c.node_id, src)
      self.assertEqual(arg1, a1)
      self.assertEqual(arg2, a2)
      return func_return

    @wrap
    def response():
      proxy = SingleRpcProxy(address, src=source, dst=destination,
                             namespace=isc_rpc_namespace, timeout=timeout,
                             ssl_options=engine.identity.get_ssl_options())
      f = proxy.handle(namespace, name, arg1, arg2)
      f.get_value(callback)

    with open(engine.identity.get_ssl_options()['certfile']) as f:
      engine.identity.register(src, f.read())
    engine.isc.register_engine_endpoint(func, namespace, name)
    self.ioloop().add_callback(response)
    self.start()

  def test_routing_app_to_other_app(self):
    """ test routing A -> N -> N' -> A'
    """
    namespace = 'namespcae'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = 'return'
    src = 'src'

    address = engine.node.__DATA__.localhost
    source = build_location(src)
    destination = build_location(engine.node.ident(), clique.runtime.app_name())
    isc_rpc_namespace = clique.isc.ISC_RPC_NAMESPACE
    timeout = 10

    @wrap
    def callback(result):
      self.assertEqual(result, func_return)
      self.stop()

    def func(a1, a2):
      with clique.isc.context() as c:
        self.assertEqual(c.node_id, src)
      self.assertEqual(arg1, a1)
      self.assertEqual(arg2, a2)
      return func_return

    @wrap
    def response(result):
      proxy = SingleRpcProxy(address, src=source, dst=destination,
                             namespace=isc_rpc_namespace, timeout=timeout,
                             ssl_options=engine.identity.get_ssl_options())
      f = proxy.handle(namespace, name, arg1, arg2)
      f.get_value(callback)

    with open(engine.identity.get_ssl_options()['certfile']) as f:
      engine.identity.register(src, f.read())
    f = clique.isc.register_endpoint(func, namespace, name)
    f.get_value(response)
    self.start()

  def test_routing_invalid_destination(self):
    """ test invalid destination error
    if destination split result's length is not 2,
    InvalidDestinationError will be raised
    """
    namespace = 'namespcae'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'

    @wrap
    def callback(result):
      self.assertTrue(isinstance(result, InvalidDestinationError))
      self.stop()

    ep = Endpoint(namespace, name, engine.node.ident())
    clique.isc.build_location = lambda n, a: 'invalid'
    future = ep(arg1, arg2)
    future.get_value(callback)

    self.start()

  def test_destination_notfound(self):
    """ test destination not found error
    if address from get_address method in engine.node is None,
    DestinationNotFoundError will be raised
    """
    namespace = 'namespcae'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'

    @wrap
    def callback(result):
      self.assertTrue(isinstance(result, DestinationNotFoundError))
      self.stop()

    ep = Endpoint(namespace, name, 'unknown')
    future = ep(arg1, arg2)
    future.get_value(callback)

    self.start()

  def test_endpoint(self):
    """Tests of basic endpoint flow
    """
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = "return"
    query = {'namespace': namespace,
             'name': name}

    @wrap
    def callback(result):
      self.assertEqual(result, func_return)
      self.stop()

    def func(a1, a2):
      self.assertEqual(arg1, a1)
      self.assertEqual(arg2, a2)
      return func_return

    @wrap
    def response(result):
      eps = engine.isc.find(query)
      future = eps[0](arg1, arg2)
      future.get_value(callback)

    f = clique.isc.register_endpoint(func, namespace, name)
    f.get_value(response)
    self.start()

  def test_find(self):
    """Test find endpoints method
    """
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = 'return'
    query = {'namespace': namespace,
             'name': name}

    @wrap
    def callback(result):
      self.assertEqual(result, func_return)
      self.stop()

    def func(a1, a2):
      self.assertEqual(arg1, a1)
      self.assertEqual(arg2, a2)
      return func_return

    @wrap
    def response(result):
      def endpoints_callback(eps):
        ep = eps[0]
        self.assertEqual(ep.namespace, namespace)
        self.assertEqual(ep.name, name)
        future = ep(arg1, arg2)
        future.get_value(callback)

      find_future = clique.isc.find(query)
      find_future.get_value(endpoints_callback)

    f = clique.isc.register_endpoint(func, namespace, name)
    f.get_value(response)
    self.start()

  def test_find_sync(self):
    """Test find endpoints sync method
    """
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = 'return'
    query = {'namespace': namespace,
             'name': name}

    @wrap
    def callback(result):
      self.assertEqual(result, func_return)
      self.stop()

    def func(a1, a2):
      self.assertEqual(arg1, a1)
      self.assertEqual(arg2, a2)
      return func_return

    @wrap
    def response(result):
      def run():
        eps = clique.isc.find_sync(query)
        future = eps[0](arg1, arg2)
        future.get_value(callback)
      Thread(target=run).start()

    f = clique.isc.register_endpoint(func, namespace, name)
    f.get_value(response)
    self.start()
