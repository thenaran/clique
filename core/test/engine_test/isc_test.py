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


"""Test cases for ISC in engine.
"""

import os
from nose.plugins.attrib import attr

from rpclib.testing import RpcTestCase
from rpclib.stackcontext import wrap
from adt.config import JsonSettings
from adt.concurrency import Lazy
from adt.funcs import try_execute
import engine.isc
import clique.isc
import clique.runtime
import engine.node
import engine.runtime
import engine.first
import rpclib
from engine.runtime import Context
from engine.isc import endpoint
from engine.isc import register_app_endpoint
from engine.isc import get_api_version
from clique.isc import Endpoint
from clique.isc import IscRegisterError


@attr(species="clique", genus="core", family="engine", name="isc")
class EndpointTestCase(RpcTestCase):
  """Tests for engine ISC.
  """
  # TODO: insert and remove db.

  def setUp(self):
    RpcTestCase.setUp(self)
    engine.first.generate_rsa_keys()
    self._old_add_service_route = engine.isc.add_service_route
    engine.isc.add_service_route = lambda appname: None
    self.old_get_unix_socket_in_path = engine.isc.get_unix_socket_in_path
    self.old_execute_cmd = engine.aengel.execute_cmd
    self.old_call = clique.isc.Endpoint.__call__
    clique.isc.Endpoint.__call__ = engine.isc.__call__
    engine.isc.get_unix_socket_in_path = \
        lambda a: clique.isc.INTERNAL_SOCKET_IN_PATH
    engine.aengel.execute_cmd = lambda c: None
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'test.config')
    info_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'test.info')
    engine.runtime.__DATA__.context = Context([info_path], [config_path])
    clique.runtime.__DATA__.context = JsonSettings([config_path])
    engine.isc.start()

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
    try_execute(os.remove, engine.identity.__DATA__.ident_db_path)
    try_execute(os.remove, engine.runtime.context().rebase_to_data('.auth'))

  def test_endpoint(self):
    """Tests of basic flow, register endpoint and fire function.
    """
    '''expected'''
    namespace = 'namespace'
    name = 'name'
    arg1 = 'arg1'
    arg2 = 'arg2'
    func_return = "return"
    query = {'namespace': namespace, 'name': name}

    '''process'''
    @endpoint(namespace, name)
    def func(a1, a2):
      '''actual'''
      self.assertEqual(arg1, a1)
      self.assertEqual(arg2, a2)
      return func_return

    @wrap
    def callback(result):
      '''actual'''
      self.assertEqual(result, func_return)
      self.stop()

    def post_condition():
      # get eps.
      eps = engine.isc.find(query)
      future = eps[0](arg1, arg2)
      future.get_value(callback)

    with open(engine.identity.get_ssl_options()['certfile']) as f:
      engine.identity.register(engine.node.ident(), f.read())
    self.ioloop().add_callback(wrap(post_condition))
    self.start()
    # TODO: add local only

  def test_handle_public(self):
    """Tests of handle public endpoint.
    """
    '''expected'''
    namespace = 'tns'
    name = 'tnm'
    func_result = 'trs'

    '''process'''
    @endpoint(namespace, name)
    def func():
      return func_result

    @wrap
    def post_condition():
      context = Lazy()
      with open(engine.identity.get_ssl_options()['certfile']) as f:
        context.public_key = f.read()
      context.source = '%s.test' % engine.node.ident()
      context.destination = ''
      rpclib._set_context(context)
      func_result = engine.isc.handle_public(namespace, name)
      '''actual'''
      self.assertEqual(func_result, func_result)
      self.stop()

    with open(engine.identity.get_ssl_options()['certfile']) as f:
      engine.identity.register(engine.node.ident(), f.read())
    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_auth_level_public(self):
    """Tests auth level public when endpoint is elevated
    """
    old_level = engine.isc.__DATA__.default_free_mode
    engine.isc.__DATA__.default_free_mode = True
    '''expected'''
    namespace = 'tns'
    name = 'tnm'
    func_result = 'trs'

    '''process'''
    @endpoint(namespace, name, elevated=True)
    def func():
      return func_result

    @wrap
    def post_condition():
      context = Lazy()
      with open(engine.identity.get_ssl_options()['certfile']) as f:
        context.public_key = f.read()
      context.source = '%s.test' % engine.node.ident()
      context.destination = ''
      rpclib._set_context(context)
      func_result = engine.isc.handle_public(namespace, name)
      '''actual'''
      self.assertEqual(func_result, func_result)
      engine.isc.__DATA__.default_free_mode = old_level
      self.stop()

    with open(engine.identity.get_ssl_options()['certfile']) as f:
      engine.identity.register(engine.node.ident(), f.read())
    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_register_without_args(self):
    """Test of register endpoint without endpoint arguments.
    """
    '''expected'''
    func_count = 1
    func_result = "result"

    @endpoint()
    def func():
      return func_result

    '''expected'''
    namespace = func.__module__
    name = func.__name__
    query = {'namespace': namespace,
             'name': name
             }

    @wrap
    def post_condition():
      '''process'''
      eps = engine.isc.find(query)

      '''actual'''
      self.assertEqual(len(eps), func_count)
      self.assertEqual(eps[0].namespace, namespace)
      self.assertEqual(eps[0].name, name)
      self.stop()
    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_register_func_args(self):
    """Tests of register function args, check correctly function arguments,
    descriptions and requried arguments.
    """
    '''expected'''
    name1 = 'n1'
    name2 = 'n2'
    arg_desc = {'arg1': 'a1', 'arg2': 'a2'}
    req_arg_func_name = 'req_func'
    non_req_args = ['na1', 'na2']
    req_arg_total_count = 5
    req_args = ['arg0', 'arg1', 'arg2']
    name1_query = {'name': name1}
    req_arg_query = {'name': req_arg_func_name}

    '''process'''
    @endpoint(name=name1, arg_descriptions=arg_desc)
    def func(arg1, arg2):
      pass

    @endpoint(name=req_arg_func_name)
    def req_args_func(arg0, arg1, arg2, arg3=non_req_args[0],
                      arg4=non_req_args[1]):
      pass

    def register_func():
      @endpoint(name=name2, arg_descriptions=arg_desc)
      def wrong_func_args(arg1):
        pass

    @wrap
    def do_test():
      arg_func_eps = engine.isc.find(name1_query)
      req_arg_func_eps = engine.isc.find(req_arg_query)
      result_req_args = []
      result_args = req_arg_func_eps[0].arg_types
      for arg in result_args:
        if result_args.get(arg) == 'required':
          result_req_args.append(arg)

      '''actual'''
      self.assertEqual(arg_func_eps[0].name, name1)
      self.assertEqual(req_arg_func_eps[0].name, req_arg_func_name)
      self.assertEqual(len(req_arg_func_eps[0].arg_types), req_arg_total_count)
      self.assertListEqual(result_req_args, req_args)
      with self.assertRaises(IscRegisterError):
        register_func()
      self.stop()
    self.ioloop().add_callback(wrap(do_test))
    self.start()

  def test_register_from_app(self):
    """Tests of register endpoint from app.
    """
    '''expect'''
    namespaces = []
    names = []
    appnames = []
    for i in range(4):
      namespaces.append('ns%d' % i)
      names.append('nm%d' % i)
      appnames.append('anm%d' % i)

    '''process'''
    # Note, the register endpoint from engine, the engine will be sets the node name.
    normal_ep = Endpoint(namespaces[0], names[0], engine.node.ident(),
                         appnames[0])
    context = Lazy()
    context.appname = appnames[0]
    clique.isc._set_context(context)
    register_app_endpoint(normal_ep)
    no_namespace_ep = Endpoint(None, names[1], engine.node.ident(),
                               appnames[1])
    no_nodename_ep = Endpoint(namespaces[2], None, engine.node.ident(),
                              appnames[2])
    empty_ep = Endpoint(None, None, None, None)

    query = {'namespace': namespaces[0], 'name': names[0],
             'appname': appnames[0]}

    @wrap
    def do_test():
      registered_normal_ep = engine.isc.find(query)
      '''assert'''
      # Check normal endpoint.
      self.assertEqual(registered_normal_ep[0].namespace, namespaces[0])
      self.assertEqual(registered_normal_ep[0].name, names[0])
      # Register wrong endpoints.
      # missing namespace.
      with self.assertRaises(IscRegisterError):
        register_app_endpoint(no_namespace_ep)
      # missing node.
      with self.assertRaises(IscRegisterError):
        register_app_endpoint(no_nodename_ep)
      # register empty endpoint.
      with self.assertRaises(IscRegisterError):
        register_app_endpoint(empty_ep)
      # register None.
      with self.assertRaises(IscRegisterError):
        register_app_endpoint(None)
      self.stop()
    self.ioloop().add_callback(wrap(do_test))
    self.start()

  def test_find_all(self):
    """Tests of find all registered eps.
    """

    '''expected'''
    namespaces = ["tns1", "tns2", "tns3"]

    '''process'''
    @endpoint(namespaces[0])
    def func1():
      pass

    @endpoint(namespaces[1])
    def func2():
      pass

    @endpoint(namespaces[2])
    def func3():
      pass

    @wrap
    def do_test():
      eps = engine.isc.find({})
      reg_eps = []
      for ep in eps:
        if ep.namespace in namespaces:
          reg_eps.append(ep)
      '''actual'''
      self.assertIsNotNone(eps)
      self.assertTrue(len(reg_eps), len(namespaces))
      self.stop()
    self.ioloop().add_callback(do_test)
    self.start()

  def test_api_version(self):
    """Tests of api version.
    """
    ori_version = [0]

    def pre_condition():
      ori_version[0] = get_api_version()
    self.ioloop().add_callback(pre_condition)

    '''process'''
    @endpoint()
    def func1():
      pass

    # register new endpoint for check update version.
    @endpoint()
    def func2():
      pass

    @endpoint()
    def func3():
      pass

    def post_condition():
      updated_version = get_api_version()
      '''actual'''
      self.assertEqual(ori_version[0] + 3, updated_version)  # added 3 endpoints.
      self.stop()
    self.ioloop().add_callback(wrap(post_condition))
    self.start()

  def test_find_query(self):
    """Test of find endpoints by query.
    """
    '''expected'''
    namespace = 'test_ns1'
    name = 'test_name1'
    namespace_count = 1
    name_total_count = 3
    name_range_count = 2
    name_over_count = 100
    elevated_count = 2
    private_name_count = 2
    namespace_query = {'namespace': namespace}
    name_query = {'name': name}
    name_count_query = {'name': name,
                        'count': name_range_count
                        }
    name_overcount_query = {'name': name,
                            'count': name_over_count
                            }
    elevated_query = {'elevated': True}
    name_elevated_query = {
        'name': name,
        'elevated': True
    }
    name_local_query = {'name': name, 'protection': clique.isc.LOCALONLY}

    '''process'''
    @endpoint(namespace, elevated=True)
    def func1():
      pass

    @endpoint('tns1', name)
    def func2():
      pass

    @endpoint('tns2', name, protection=clique.isc.LOCALONLY)
    def func3():
      pass

    @endpoint('tns3', name, elevated=True, protection=clique.isc.LOCALONLY)
    def test_func4():
      pass

    @wrap
    def do_test():
      # query for namespace.
      ns_eps = engine.isc.find(namespace_query)
      # query for node.
      name_eps = engine.isc.find(name_query)
      # query for name with count.
      namecount_eps = engine.isc.find(name_count_query)
      # query for name with over count.
      nameovercount_eps = engine.isc.find(name_overcount_query)
      # query for only elevated endpoints.
      ele_eps = engine.isc.find(elevated_query)
      # query for name with elevated.
      elevated_eps = engine.isc.find(name_elevated_query)
      # for locations.
      # query for local only with name.
      private_eps = engine.isc.find(name_local_query)

      '''actual'''
      self.assertTrue(len(ns_eps) == namespace_count)
      self.assertEqual(ns_eps[0].namespace, namespace)
      self.assertTrue(len(name_eps) == name_total_count)
      map(lambda f: self.assertEqual(f.name, name), name_eps)
      self.assertTrue(len(namecount_eps) == name_range_count)
      map(lambda f: self.assertEqual(f.name, name), namecount_eps)
      self.assertTrue(len(nameovercount_eps) == name_total_count)
      map(lambda f: self.assertEqual(f.name, name), nameovercount_eps)
      self.assertTrue(len(ele_eps) == elevated_count)
      map(lambda f: self.assertTrue(f.elevated), ele_eps)
      self.assertTrue(len(elevated_eps) == 1)
      self.assertTrue(len(private_eps) == private_name_count)
      map(lambda f: self.assertEqual(f.name, name), private_eps)
      self.stop()
    self.ioloop().add_callback(do_test)
    self.start()


if  __name__ == "__main__":
  from unittest import main
  main()
