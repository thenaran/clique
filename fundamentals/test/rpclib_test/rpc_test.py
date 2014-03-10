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


""" RPC proxy and server tests.
"""
import io
import logging
import time
import subprocess
import functools

import rpclib
import rpclib.server
import rpclib.proxy
from rpclib.proxy import RpcMethodNotFoundError
from rpclib.proxy import DestinationNotFoundError
from rpclib.proxy import NameSpaceNotFoundError
from rpclib.proxy import TimeoutError
from rpclib.proxy import generalize, specialize
from rpclib.proxy import SingleRpcProxy
from rpclib.proxy import BinaryRpcProxy
from rpclib.proxy import SharedRpcProxy
from rpclib.proxy import Binary
from rpclib.proxy import RpcProxyCollection
from rpclib.proxy import BinaryToFileCopier
from rpclib.proxy import FileToBinaryCopier
from rpclib.proxy import ReadStream
from rpclib.proxy import WriteStream
from rpclib.proxy import SHARED_REQUEST
from rpclib.server import WebSocket
from rpclib.server import RpcServerOverWebSocket
from rpclib.server import RpcServerOverTcp
from rpclib.server import RpcServerOverUnixSocket
from rpclib.server import RoutingInfo
from rpclib.stackcontext import wrap
from rpclib.testing import RpcTestCase
from adt.concurrency import Future
from adt.concurrency import FutureCollection
from adt.concurrency import Lazy
from nose.plugins.attrib import attr
from cStringIO import StringIO
from threading import Thread
import os
import tempfile
import random
import ssl
import shutil
import ast


MAX_RUNNING_TIME = 10   # Maximum test running time.
SERVER_ONE_ID = '''server1'''
SERVER_TWO_ID = '''server2'''


class TestBase(rpclib.proxy.Base):
  def __init__(self, int_arg, string_arg, boolean_arg):
    self.int_arg = int_arg
    self.string_arg = string_arg
    self.boolean_arg = boolean_arg
    self._private_int = 0


@attr(species="clique", genus="fundamentals", family="rpclib", name="base")
class BaseTypeTests(RpcTestCase):
  """Generalizable/specializable base type tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self._int = 1
    self._string = 'string'
    self._boolean = True
    self._int_arg_key = 'int_arg'
    self._string_arg_key = 'string_arg'
    self._boolean_arg_key = 'boolean_arg'
    self._classname = 'TestBase'
    self._modulename = 'fundamentals.rpclib_test.rpc_test'

  def tearDown(self):
    RpcTestCase.tearDown(self)

  def _generate_list(self):
    return [self._int, self._string, self._boolean]

  def _generate_primitive(self):
    return {self._int_arg_key: self._int, self._string_arg_key: self._string,
            self._boolean_arg_key: self._boolean}

  def _generate_known_primitive(self):
    prim = self._generate_primitive()
    prim['___classname___'] = self._classname
    prim['___module___'] = self._modulename
    return prim

  def _generate_single_obj(self):
    return TestBase(self._int, self._string, self._boolean)

  def test_generalize_primitives(self):
    obj = self._generate_primitive()

    result = generalize(obj)

    self.assertIsNotNone(result)
    self.assertTrue(isinstance(result, dict))
    self.assertEqual(obj[self._int_arg_key], result[self._int_arg_key])
    self.assertEqual(obj[self._string_arg_key], result[self._string_arg_key])
    self.assertEqual(obj[self._boolean_arg_key], result[self._boolean_arg_key])

  def test_generalize_single_obj(self):
    obj = TestBase(self._int, self._string, self._boolean)
    obj._private_int = self._int

    result = generalize(obj)

    self.assertIsNotNone(result)
    self.assertNotEqual(result, obj)
    self.assertEqual(obj.int_arg, self._int)
    self.assertEqual(obj.string_arg, self._string)
    self.assertEqual(obj.boolean_arg, self._boolean)
    self.assertEqual(obj._private_int, self._int)

    self.assertEqual(result[self._int_arg_key], self._int)
    self.assertEqual(result[self._string_arg_key], self._string)
    self.assertEqual(result[self._boolean_arg_key], self._boolean)
    self.assertIsNone(result.get('_private_int'))

  def test_generalize_nested_obj(self):
    obj = self._generate_single_obj()
    obj.list_arg = [self._int, self._string, self._boolean,
                    self._generate_single_obj()]
    obj.map_arg = {'child': self._generate_single_obj()}
    obj.child = self._generate_single_obj()
    obj.child.child = self._generate_single_obj()

    result = generalize(obj)

    self.assertIsNotNone(result)
    self.assertNotEqual(result, obj)
    self.assertEqual(result['list_arg'][0], self._int)
    self.assertEqual(result['list_arg'][1], self._string)
    self.assertEqual(result['list_arg'][2], self._boolean)
    self.assertEqual(result['list_arg'][3][self._int_arg_key], self._int)
    self.assertEqual(result['list_arg'][3][self._string_arg_key], self._string)
    self.assertEqual(result['list_arg'][3][self._boolean_arg_key],
                     self._boolean)

    self.assertEqual(result['map_arg']['child'][self._int_arg_key], self._int)
    self.assertEqual(result['map_arg']['child'][self._string_arg_key],
                     self._string)
    self.assertEqual(result['map_arg']['child'][self._boolean_arg_key],
                     self._boolean)

    self.assertEqual(result['child'][self._int_arg_key], self._int)
    self.assertEqual(result['child'][self._string_arg_key], self._string)
    self.assertEqual(result['child'][self._boolean_arg_key], self._boolean)

    self.assertEqual(result['child']['child'][self._int_arg_key], self._int)
    self.assertEqual(result['child']['child'][self._string_arg_key],
                     self._string)
    self.assertEqual(result['child']['child'][self._boolean_arg_key],
                     self._boolean)

  def test_specialize_primitives(self):
    prim = self._generate_known_primitive()

    obj = specialize(prim)

    self.assertIsNotNone(obj)
    # TODO(hdkim): check isinstance
    self.assertEqual(prim[self._int_arg_key], obj.int_arg)
    self.assertEqual(prim[self._string_arg_key], obj.string_arg)
    self.assertEqual(prim[self._boolean_arg_key], obj.boolean_arg)

  def test_specialize_single_obj(self):
    obj = self._generate_single_obj()

    result = specialize(obj)

    self.assertIsNotNone(result)
    self.assertEqual(obj, result)
    self.assertEqual(obj.int_arg, result.int_arg)
    self.assertEqual(obj.string_arg, result.string_arg)
    self.assertEqual(obj.boolean_arg, result.boolean_arg)

  def test_specialize_nested_obj(self):
    obj = self._generate_single_obj()
    obj.list_arg = [self._int, self._string, self._boolean,
                    self._generate_single_obj()]
    obj.map_arg = {'child': self._generate_single_obj()}
    obj.child = self._generate_single_obj()
    obj.child.child = self._generate_single_obj()

    result = specialize(obj)

    self.assertIsNotNone(result)
    self.assertEqual(obj, result)
    self.assertEqual(obj.list_arg, result.list_arg)
    self.assertEqual(obj.map_arg, result.map_arg)
    self.assertEqual(obj.child, result.child)
    self.assertEqual(obj.child.child, result.child.child)

  def test_specialize_unknown_typed_obj(self):
    prim = self._generate_primitive()
    prim['___classname___'] = 'unknown'
    prim['___module___'] = 'unknown'

    result = specialize(prim)

    self.assertIsNotNone(result)
    self.assertTrue(isinstance(result, rpclib.proxy.Base))
    self.assertEqual(prim[self._int_arg_key], result.int_arg)
    self.assertEqual(prim[self._string_arg_key], result.string_arg)
    self.assertEqual(prim[self._boolean_arg_key], result.boolean_arg)

  def test_objs_in_list(self):
    """Test generalize/specialize objects in lists.
    """
    list_obj = [self._generate_single_obj(), self._generate_single_obj()]

    list_result = generalize(list_obj)

    self.assertIsNotNone(list_result)
    self.assertEqual(len(list_result), 2)
    self.assertEqual(list_obj[0].int_arg,
                     list_result[0][self._int_arg_key])
    self.assertEqual(list_obj[0].string_arg,
                     list_result[0][self._string_arg_key])
    self.assertEqual(list_obj[0].boolean_arg,
                     list_result[0][self._boolean_arg_key])

    list_prim_obj = [self._generate_known_primitive(),
                     self._generate_known_primitive()]

    list_prim_result = specialize(list_prim_obj)

    self.assertIsNotNone(list_prim_result)
    self.assertEqual(len(list_prim_result), 2)
    self.assertEqual(list_prim_obj[0].int_arg,
                     self._int)
    self.assertEqual(list_prim_obj[0].string_arg,
                     self._string)
    self.assertEqual(list_prim_obj[0].boolean_arg,
                     self._boolean)

  def test_objs_in_dict(self):
    """Test generalize/specialize objects in dictionaries.
    """
    dic_obj = {'obj1': self._generate_single_obj(),
               'obj2': self._generate_single_obj()}

    dic_result = generalize(dic_obj)
    self.assertIsNotNone(dic_result)
    self.assertEqual(len(dic_result), 2)
    self.assertEqual(dic_obj['obj1'].int_arg,
                     dic_result['obj1'][self._int_arg_key])
    self.assertEqual(dic_obj['obj1'].string_arg,
                     dic_result['obj1'][self._string_arg_key])
    self.assertEqual(dic_obj['obj1'].boolean_arg,
                     dic_result['obj1'][self._boolean_arg_key])

    dic_prim_obj = {'obj1': self._generate_known_primitive(),
                    'obj2': self._generate_known_primitive()}

    dic_prim_result = specialize(dic_prim_obj)

    self.assertIsNotNone(dic_prim_result)
    self.assertEqual(len(dic_prim_result), 2)
    self.assertEqual(dic_prim_obj['obj1'].int_arg, self._int)
    self.assertEqual(dic_prim_obj['obj1'].string_arg, self._string)
    self.assertEqual(dic_prim_obj['obj1'].boolean_arg, self._boolean)

  def test_none_base_obj(self):
    class NoneBase(object):
      def __init__(self, a, b):
        self.a = a
        self.b = b

    obj = NoneBase(1, 2)

    result = generalize(obj)

    self.assertIsNotNone(result)
    self.assertEqual(result.a, 1)
    self.assertEqual(result.b, 2)

    specialized = specialize(result)

    self.assertIsNotNone(specialized)
    self.assertEqual(specialized.a, 1)
    self.assertEqual(specialized.b, 2)


class RpcCallMixin(object):
  def test_simple_call(self):
    """Simple async method call test.
    """
    logging.debug("start simple call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      logging.debug("return value")
      return retval

    self.server.register_function(simple_method)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_simple_sync_call(self):
    """Simple async method call test.
    """
    logging.debug("start simple call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      logging.debug("return value")
      return retval

    self.server.register_function(simple_method)

    def callback():
      proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                             ssl_options=self._client_ssl_options)
      result = rpclib.proxy.sync(proxy.simple_method)(*args)
      self.assertEqual(result, retval)
      self.stop()

    Thread(target=callback).start()
    self.start(MAX_RUNNING_TIME)

  def test_instance_calls(self):
    """Simple async method in instance call test
    """
    logging.debug("start instance call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    testor = self

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.server.register_instance(Test())

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_namespace_call(self):
    """Simple async method call test that has namespace
    """
    logging.debug("start namespace call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method, namespace=namespace)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           namespace=namespace,
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_namespace_instance_call(self):
    """Simple async method in instance call test that has namespace
    """
    logging.debug("start name space instance test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'
    testor = self

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.server.register_instance(Test(), namespace=namespace)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           namespace=namespace,
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_async_func(self):
    """Test the async remote method.
    """
    logging.debug("start async call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def async_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def async():
        f.set_value(retval)
      self.ioloop().add_callback(async)
      return f

    self.server.register_function(async_method)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.async_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_tuple_result_func(self):
    """Simple async method routing call test. in routing
    """
    logging.debug("start tuple simple call test")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return (retval, retval)

    self.server.register_function(simple_method)

    def response(result):
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1], retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_tuple_async_result_func(self):
    """Test the async remote method in routing.
    """
    logging.debug("start tuple async call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def async_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def async():
        f.set_value((retval, retval))
      self.ioloop().add_callback(async)
      return f

    self.server.register_function(async_method)

    def response(result):
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1], retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.async_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_rpc_collection(self):
    """Test the rpc proxy collection
    """
    logging.debug("start rpc collection request test")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    call_count = 5

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method)

    def response(result):
      self.assertEqual(len(result), call_count)
      for value in result:
        self.assertEqual(value, retval)
      self.stop()

    addresses = []
    for i in range(call_count):
      addresses.append(self.address)

    proxy = RpcProxyCollection(addresses, ioloop=self.ioloop(),
                               ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)


class BinaryRpcCallMixin(object):
  def test_raw_output(self):
    """Test the raw output remote method.
    """
    logging.debug("start raw output test")
    self.server.clear()
    data = r'010101010101' * 100
    args = [10, 'string', True]

    def raw_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def stream_ready(binary):
        logging.debug("stream ready")
        binary.write(data)

      f.set_value(Binary(len(data), stream_ready))
      return f

    self.server.register_function(raw_method)

    def response(result):
      logging.debug("binary result:%s", str(result))
      self.assertTrue(isinstance(result, Binary))
      self.assertEqual(result.size, len(data))

      def check_data(received):
        logging.debug("check data")
        self.assertEqual(received, data)

      def finish():
        logging.debug("finish!!")
        result.finish()
        self.stop()
      result.read(check_data, finish, result.size)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_re_raw_output(self):
    """Test the raw output remote method.
    """
    logging.debug("start raw output test")
    self.server.clear()
    data = r'010101010101' * 100
    args = [10, 'string', True]

    def raw_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def stream_ready(binary):
        binary.write(data)

      f.set_value(Binary(len(data), stream_ready))
      return f

    self.server.register_function(raw_method)

    def response(result):
      self.assertTrue(isinstance(result, Binary))
      self.assertEqual(result.size, len(data))

      def check_data(received):
        self.assertEqual(received, data)

      def finish():
        logging.debug("finish result")
        result.finish()

        def response2(result2):
          self.assertTrue(isinstance(result2, Binary))
          self.assertEqual(result2.size, len(data))

          def check_data2(received2):
            self.assertEqual(received2, data)

          def finish2():
            logging.debug("finish result2")
            result2.finish()
            self.stop()

          result2.read(check_data2, finish2, result2.size)

        proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                               ssl_options=self._client_ssl_options)
        f = proxy.raw_method(*args)
        f.get_value(wrap(response2))

      result.read(check_data, finish, result.size)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_tuple_raw_output(self):
    """Test the tuple raw output remote method in routing.
    """
    logging.debug("start tuple raw output test")
    self.server.clear()
    data = r'010101010101' * 100
    args = [10, 'string', True]
    retval = 100

    def raw_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def ready_callback(binary):
        binary.write(data)
        binary.finish()

      f.set_value((retval, Binary(len(data), ready_callback)))
      return f

    self.server.register_function(raw_method)

    def response(result):
      self.assertTrue(isinstance(result[1], Binary))
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1].size, len(data))

      def check_data(received):
        self.assertEqual(received, data)

      def finish():
        result[1].finish()
        self.stop()

      result[1].read(check_data, finish, result[1].size)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_raw_input(self):
    """Test the remote method acception a raw input data.
    """
    logging.debug("start raw input test")
    self.server.clear()
    data = r'010101010101' * 100
    args = [10, 'string', True]
    retval = 100

    def raw_method(input_binary, i, s, b):
      logging.debug("raw method")
      self.assertTrue(isinstance(input_binary, Binary))
      self.assertEqual(input_binary.size, len(data))
      f = Future()

      def check_data(received):
        logging.debug("check data")
        self.assertEqual(received, data)

      def finish():
        logging.debug("input finish")
        f.set_value(retval)
        input_binary.finish()

      input_binary.read(check_data, finish, input_binary.size)
      return f

    self.server.register_function(raw_method)

    def handle_input(binary):
      logging.debug("input binary finish")
      binary.write(data)
      binary.finish()

    def response(result):
      logging.debug("response result:%s", str(result))
      self.assertEqual(result, retval)
      self.stop()

    input_binary = Binary(len(data), handle_input)
    args = [input_binary] + args

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_raw_input_output(self):
    """Test the remote method raw input and output data
    """
    logging.debug("start raw input and ouput test")
    self.server.clear()
    data = r'010101010101' * 100
    args = [10, 'string', True]

    def raw_method(input_binary, i, s, b):
      self.assertTrue(isinstance(input_binary, Binary))
      self.assertEqual(input_binary.size, len(data))
      f = Future()

      def check_data(received):
        self.assertEqual(data, received)

        def ready_callback(binary):
          binary.write(received)
          binary.finish()

        output_binary = Binary(len(received), ready_callback)
        f.set_value(output_binary)

      def finish():
        input_binary.finish()

      input_binary.read(check_data, finish, input_binary.size)
      return f

    self.server.register_function(raw_method)

    def handle_input(binary):
      binary.write(data)
      binary.finish()

    def handle_output(binary):
      self.assertTrue(isinstance(binary, Binary))
      self.assertEqual(binary.size, len(data))

      def check_data(received):
        self.assertEqual(received, data)

      def finish():
        binary.finish()
        self.stop()
      binary.read(check_data, finish, binary.size)

    input_binary = Binary(len(data), handle_input)
    args.insert(0, input_binary)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(handle_output))
    self.start(MAX_RUNNING_TIME)

  def test_raw_re_input_output(self):
    """Test the remote method raw input and output data
    """
    logging.debug("start raw input and ouput test")
    self.server.clear()
    data = r'010101010101' * 100
    args = [10, 'string', True]

    def raw_method(input_binary, i, s, b):
      self.assertTrue(isinstance(input_binary, Binary))
      self.assertEqual(input_binary.size, len(data))
      f = Future()

      def check_data(received):
        self.assertEqual(data, received)

        def ready_callback(binary):
          binary.write(received)
          binary.finish()

        output_binary = Binary(len(received), ready_callback)
        f.set_value(output_binary)

      def finish():
        input_binary.finish()

      input_binary.read(check_data, finish, input_binary.size)
      return f

    self.server.register_function(raw_method)

    def handle_input(binary):
      binary.write(data)
      binary.finish()

    def handle_output(binary):
      self.assertTrue(isinstance(binary, Binary))
      self.assertEqual(binary.size, len(data))

      def check_data(received):
        self.assertEqual(received, data)

      def finish():
        logging.debug("finish binary read and finish binary")
        binary.finish()

        def handle_output2(binary2):
          self.assertTrue(isinstance(binary2, Binary))
          self.assertEqual(binary2.size, len(data))

          def check_data2(received2):
            self.assertEqual(received2, data)

          def finish2():
            binary2.finish()
            self.stop()

          binary2.read(check_data2, finish2, binary2.size)

        args = [10, 'string', True]
        input_binary = Binary(len(data), handle_input)
        args.insert(0, input_binary)

        proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                               ssl_options=self._client_ssl_options)
        logging.debug("request ")
        f = proxy.raw_method(*args)
        f.get_value(wrap(handle_output2))

      binary.read(check_data, finish, binary.size)

    input_binary = Binary(len(data), handle_input)
    args.insert(0, input_binary)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(handle_output))
    self.start(MAX_RUNNING_TIME)

  def test_raw_output_streaming(self):
    """Test the remote method raw output streaming data
    """
    logging.debug("start raw output streaming test")
    self.server.clear()
    data = r'010101010101' * 1000
    chunk_size = 100  # default tornado chunk size
    args = [10, 'string', True]

    def raw_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def ready_callback(binary):
        bytes_io = io.BytesIO(data)
        byte = bytes_io.read(chunk_size)
        size = 0
        while len(byte) > 0:
          size += len(byte)
          binary.write(byte)
          byte = bytes_io.read(chunk_size)

      output = Binary(len(data), ready_callback)

      f.set_value(output)
      return f

    self.server.register_function(raw_method)

    #check_count = 0
    #check_remain_chunk_size = 0
    results = StringIO()

    def handle_output(binary):
      self.assertTrue(isinstance(binary, Binary))
      self.assertEqual(binary.size, len(data))

      def streaming_callback(received):
        results.write(str(received))

      def finish_callback():
        self.assertEqual(results.getvalue(), data)
        binary.finish()
        self.stop()

      binary.read(streaming_callback, finish_callback, binary.size)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(handle_output))
    self.start(MAX_RUNNING_TIME)

  def test_raw_output_re_streaming(self):
    """Test the remote method raw output streaming data
    """
    logging.debug("start raw output streaming test")
    self.server.clear()
    data = r'010101010101' * 1000
    chunk_size = 100  # default tornado chunk size
    args = [10, 'string', True]

    def raw_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def ready_callback(binary):
        bytes_io = io.BytesIO(data)
        byte = bytes_io.read(chunk_size)
        size = 0
        while len(byte) > 0:
          size += len(byte)
          binary.write(byte)
          byte = bytes_io.read(chunk_size)

      output = Binary(len(data), ready_callback)

      f.set_value(output)
      return f

    self.server.register_function(raw_method)

    #check_count = 0
    #check_remain_chunk_size = 0
    results = StringIO()

    def handle_output(binary):
      self.assertTrue(isinstance(binary, Binary))
      self.assertEqual(binary.size, len(data))

      def streaming_callback(received):
        results.write(str(received))

      def finish_callback():
        self.assertEqual(results.getvalue(), data)
        binary.finish()

        results2 = StringIO()

        def handle_output2(binary2):
          self.assertTrue(isinstance(binary2, Binary))
          self.assertEqual(binary2.size, len(data))

          def streaming_callback2(received2):
            results2.write(str(received2))

          def finish_callback2():
            self.assertEqual(results2.getvalue(), data)
            binary2.finish()

          binary2.read(streaming_callback2, finish_callback2, binary2.size)

        proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                               ssl_options=self._client_ssl_options)
        f = proxy.raw_method(*args)
        f.get_value(wrap(handle_output2))

        self.stop()

      binary.read(streaming_callback, finish_callback, binary.size)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(handle_output))
    self.start(MAX_RUNNING_TIME)

  def test_raw_intput_streaming(self):
    """Test the remove method raw input streaming data
    """
    logging.debug("start raw input streaming test")
    self.server.clear()
    data = r'010101010101' * 1000
    chunk_size = 100
    args = [10, 'string', True]
    retval = 100

    results = StringIO()

    def raw_method(input_binary, i, s, b):
      self.assertEqual(input_binary.size, len(data))
      f = Future()

      def streaming_callback(received):
        results.write(str(received))

      def finish_callback():
        f.set_value(retval)
        input_binary.finish()
        self.stop()
      input_binary.read(streaming_callback, finish_callback, input_binary.size)
      return f

    self.server.register_function(raw_method)

    def handle_input(binary):
      bytes_io = io.BytesIO(data)
      byte = bytes_io.read(chunk_size)
      while len(byte) > 0:
        binary.write(byte)
        byte = bytes_io.read(chunk_size)
      binary.finish()

    def response(result):
      self.assertEqual(result, retval)

    input_s = Binary(len(data), handle_input)
    args.insert(0, input_s)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(response))
    self.start()


class SharedRpcCallMixin(object):
  def test_simple_call(self):
    """Simple shared rpc async method call test.
    """
    logging.debug("start simple call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_instance_calls(self):
    """Simple async method in instance call test
    """
    logging.debug("start instance call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    testor = self

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.server.register_instance(Test())

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_namespace_call(self):
    """Simple async method call test that has namespace
    """
    logging.debug("start namespace call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method, namespace=namespace)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          namespace=namespace,
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_namespace_instance_call(self):
    """Simple async method in instance call test that has namespace
    """
    logging.debug("start name space instance test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'
    testor = self

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.server.register_instance(Test(), namespace=namespace)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          namespace=namespace,
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_async_func(self):
    """Test the async remote method.
    """
    logging.debug("start async call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def async_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def async():
        f.set_value(retval)
      self.ioloop().add_callback(async)
      return f

    self.server.register_function(async_method)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.async_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_tuple_result_func(self):
    """Simple async method routing call test. in routing
    """
    logging.debug("start tuple simple call test")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return (retval, retval)

    self.server.register_function(simple_method)

    def response(result):
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1], retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_tuple_async_result_func(self):
    """Test tuple async result.
    """
    logging.debug("start tuple async call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def async_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def async():
        f.set_value((retval, retval))
      self.ioloop().add_callback(async)
      return f

    self.server.register_function(async_method)

    def response(result):
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1], retval)
      self.stop()

    def callback(proxy):
      f = proxy.async_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_multi_call_single_instance(self):
    """multi call single instance.
    """
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    count = 10

    results = Lazy()
    results.result = []

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method)

    def response(result):
      results.result.append(result)
      self.assertEqual(result, retval)
      if len(results.result) == count:
        self.stop()

    def callback(proxy):
      for i in range(count):
        f = proxy.simple_method(*args)
        f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_multi_create_call(self):
    '''multi create call
    '''
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    count = 10

    results = Lazy()
    results.result = []

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method)

    def response(result):
      results.result.append(result)
      self.assertEqual(result, retval)
      self.assertEqual(len(SharedRpcProxy.__STREAMS__), 1)
      if len(results.result) == count:
        self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    for i in range(count):
      SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                            ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_multi_create_after_request(self):
    '''multi create call
    '''
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    count = 10

    results = Lazy()
    results.result = []

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method)

    def response(result):
      results.result.append(result)
      self.assertEqual(result, retval)
      self.assertEqual(len(SharedRpcProxy.__STREAMS__), 1)
      if len(results.result) == count:
        self.stop()
      else:
        create()

    def create():
      SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                            ssl_options=self._client_ssl_options).then(callback)

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    self.ioloop().add_callback(create)
    self.start(MAX_RUNNING_TIME)


@attr(species="clique", genus="fundamentals", family="rpclib", name="messagecall")
class CallTests(RpcTestCase, SharedRpcCallMixin, RpcCallMixin, BinaryRpcCallMixin):
  """Legitimate RPC call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.address = ('127.0.0.1', self.server.serve())
    self._server_ssl_options = None
    self._client_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    SharedRpcProxy.dispose_all()


@attr(species="clique", genus="fundamentals", family="rpclib", name="unixcall")
class UnixCallTests(RpcTestCase, RpcCallMixin, BinaryRpcCallMixin, SharedRpcCallMixin):
  def setUp(self):
    RpcTestCase.setUp(self)
    self.address = os.path.join(tempfile.gettempdir(), str(random.random()))
    self.server = RpcServerOverUnixSocket(path=self.address,
                                          ioloop=self.ioloop())
    self.server.serve()
    self._server_ssl_options = None
    self._client_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    SharedRpcProxy.dispose_all()


@attr(species="clique", genus="fundamentals", family="rpclib", name="sslcall")
class SslCallTests(RpcTestCase, RpcCallMixin, BinaryRpcCallMixin, SharedRpcCallMixin):
  def setUp(self):
    RpcTestCase.setUp(self)
    self._temp_dir = tempfile.mkdtemp()
    self._root_priv_path = os.path.join(
        self._temp_dir, 'server_priv.pem')
    self._root_csr_path = os.path.join(
        self._temp_dir, 'ca.cert.csr')
    self._root_cert_path = os.path.join(
        self._temp_dir, 'ca.cert')
    self._root_key_path = os.path.join(
        self._temp_dir, 'ca.key')

    _gen_ras(self._root_priv_path)
    _gen_csr(self._root_csr_path, "/C=KO/ST=SEOUL/L=SEOUL/O=Narantech Inc/OU=Software Group/CN=CLIQUE ROOT/emailAddress=narantech@narantech.com",
             self._root_priv_path)  # gen server csr
    _gen_ca_key(self._root_priv_path, self._root_key_path)  # gen server key
    _gen_ca_cert(self._root_csr_path, self._root_key_path,
                 self._root_cert_path)  # gen server cert

    self._priv_path = os.path.join(
        self._temp_dir, 'client_priv.pem')
    self._issued_csr_path = os.path.join(
        self._temp_dir, 'c1.cert.csr')
    self._issued_cert_path = os.path.join(
        self._temp_dir, 'c1.cert')
    self._issued_key_path = os.path.join(
        self._temp_dir, 'c1.key')

    _gen_ras(self._priv_path)
    _gen_csr(self._issued_csr_path, "/C=KO/ST=SEOUL/L=SEOUL/O=Narantech Inc/OU=CLIQUE/CN=CLIQUE/emailAddress=narantech@narantech.com",
             self._priv_path)
    _gen_ca_key(self._priv_path, self._issued_key_path)
    _issue_cert(self._issued_csr_path, self._issued_cert_path,
                self._issued_key_path, self._root_cert_path,
                self._root_key_path)

    self._server_ssl_options = {
      'certfile': self._root_cert_path,
      'keyfile': self._root_key_path,
      'cert_reqs': ssl.CERT_OPTIONAL,
      'ca_certs': self._root_cert_path,
      #'ssl_version': ssl.PROTOCOL_TLSv1
    }

    self._client_ssl_options = {
      'certfile': self._issued_cert_path,
      'keyfile': self._issued_key_path,
      #'ssl_version': ssl.PROTOCOL_TLSv1
    }

    self.server = RpcServerOverTcp(ioloop=self.ioloop(),
                                   ssl_options=self._server_ssl_options)
    self.address = ("127.0.0.1", self.server.serve())

    def handle_auth(token, pub_key):
      if pub_key == open(self._client_cert_path).read():
        return True
      return False

    self.server.handle_authorization = handle_auth

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    if os.path.exists(self._temp_dir):
      shutil.rmtree(self._temp_dir)
    SharedRpcProxy.dispose_all()


class StreamingCallMixin(object):
  def test_file_output_streaming(self):
    """Test streaming a file
    """
    target = 'app.target'
    target_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               target)

    def raw_method():
      def ready_callback(binary):
        FileToBinaryCopier(open(self.icon_path), binary, binary.size,
                           lambda c: None, auto_close=True).start()

      return Future(Binary(os.path.getsize(self.icon_path),
                           ready_callback))

    self.server.register_function(raw_method)

    def handle_output(binary):
      logging.debug("target path:%s", str(target_path))
      target = open(target_path, 'wb')

      def end(c):
        self.assertTrue(c.error is None)
        self.assertTrue(os.path.exists(target_path))
        self.assertTrue(os.path.exists(self.icon_path))
        with open(target_path, 'rb') as t:
          logging.debug("target size:%s, icon size:%s",
                        str(os.path.getsize(target_path)),
                        str(os.path.getsize(self.icon_path)))

          self.assertEqual(os.path.getsize(target_path),
                           os.path.getsize(self.icon_path))
          with open(self.icon_path, 'rb') as a:
            self.assertEqual(t.read(), a.read())
        os.remove(target_path)
        self.stop()
      BinaryToFileCopier(binary, target, binary.size, end,
                         auto_close=True).start()

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    proxy.raw_method().then(handle_output)
    self.start(MAX_RUNNING_TIME)

  def test_large_file_output_streaming(self):
    """Test streaming a large file
    """
    target = 'app.target'
    target_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               target)

    def raw_method():
      def ready_callback(binary):
        FileToBinaryCopier(open(self.large_path), binary, binary.size,
                           lambda c: None, auto_close=True).start()

      return Future(Binary(os.path.getsize(self.large_path),
                           ready_callback))

    self.server.register_function(raw_method)

    def handle_output(binary):
      logging.debug("target path:%s", str(target_path))
      target = open(target_path, 'wb')

      def end(c):
        self.assertTrue(c.error is None)
        self.assertTrue(os.path.exists(target_path))
        self.assertTrue(os.path.exists(self.large_path))
        with open(target_path, 'rb') as t:
          logging.debug("target size:%s, icon size:%s",
                        str(os.path.getsize(target_path)),
                        str(os.path.getsize(self.large_path)))

          self.assertEqual(os.path.getsize(target_path),
                           os.path.getsize(self.large_path))
          with open(self.large_path, 'rb') as a:
            self.assertEqual(t.read(), a.read())
        os.remove(target_path)
        self.stop()
      BinaryToFileCopier(binary, target, binary.size, end,
                         auto_close=True).start()

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    proxy.raw_method().then(handle_output)
    self.start(MAX_RUNNING_TIME)

  def test_multi_files_output_streaming(self):
    """Test streaming multi files
    """
    target = 'app.target'
    target_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               target)
    count = 20

    def raw_method():
      def ready_callback(binary):
        FileToBinaryCopier(open(self.icon_path), binary, binary.size,
                           lambda c: None, auto_close=True).start()

      return Future(Binary(os.path.getsize(self.icon_path),
                           ready_callback))

    self.server.register_function(raw_method)

    def handle_output(file_path, binary):
      logging.debug("handle out target:%s", str(file_path))
      target = open(file_path, 'wb')

      def end(path, c):
        self.assertTrue(c.error is None)
        self.assertTrue(os.path.exists(path))
        self.assertTrue(os.path.exists(self.icon_path))
        logging.debug("target size:%s, icon size:%s",
                      str(os.path.getsize(path)),
                      str(os.path.getsize(self.icon_path)))
        self.assertEqual(os.path.getsize(path),
                         os.path.getsize(self.icon_path))
        with open(path, 'rb') as t:
          with open(self.icon_path, 'rb') as a:
            self.assertEqual(t.read(), a.read())
        os.remove(path)
        self.stop()
      BinaryToFileCopier(binary, target, binary.size, functools.partial(end, file_path),
                         auto_close=True).start()

    for i in range(count):
      proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                             ssl_options=self._client_ssl_options)
      proxy.raw_method().then(functools.partial(handle_output,
                                                '%s-%s' % (target_path,
                                                           str(i))))
    self.start(MAX_RUNNING_TIME)

  def test_multi_large_files_output_streaming(self):
    """Test streaming multi files
    """
    target = 'app.target'
    target_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               target)
    count = 20

    def raw_method():
      def ready_callback(binary):
        FileToBinaryCopier(open(self.large_path), binary, binary.size,
                           lambda c: None, auto_close=True).start()

      return Future(Binary(os.path.getsize(self.large_path),
                           ready_callback))

    self.server.register_function(raw_method)

    def handle_output(file_path, binary):
      logging.debug("handle out target:%s", str(file_path))
      target = open(file_path, 'wb')

      def end(path, c):
        self.assertTrue(c.error is None)
        self.assertTrue(os.path.exists(path))
        self.assertTrue(os.path.exists(self.large_path))
        logging.debug("target size:%s, icon size:%s",
                      str(os.path.getsize(path)),
                      str(os.path.getsize(self.large_path)))
        self.assertEqual(os.path.getsize(path),
                         os.path.getsize(self.large_path))
        with open(path, 'rb') as t:
          with open(self.large_path, 'rb') as a:
            self.assertEqual(t.read(), a.read())
        os.remove(path)
        self.stop()
      BinaryToFileCopier(binary, target, binary.size, functools.partial(end, file_path),
                         auto_close=True).start()

    for i in range(count):
      proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                             ssl_options=self._client_ssl_options)
      proxy.raw_method().then(functools.partial(handle_output,
                                                '%s-%s' % (target_path,
                                                           str(i))))
    self.start(MAX_RUNNING_TIME)


@attr(species="clique", genus="fundamentals", family="rpclib", name="streamingcall")
class StreamingCallTests(RpcTestCase, StreamingCallMixin):
  """Legitimate RPC call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.icon = 'app.ico'
    self.large = 'test.tar.gz'
    self.icon_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                  self.icon)
    self.large_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                   self.large)
    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.address = ('127.0.0.1', self.server.serve())
    self._server_ssl_options = None
    self._client_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()


@attr(species="clique", genus="fundamentals", family="rpclib", name="streamingunixcall")
class UnixCallTests(RpcTestCase, StreamingCallMixin):
  def setUp(self):
    RpcTestCase.setUp(self)
    self.icon = 'app.ico'
    self.large = 'test.tar.gz'
    self.icon_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                  self.icon)
    self.large_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                   self.large)
    self.address = os.path.join(tempfile.gettempdir(), str(random.random()))
    self.server = RpcServerOverUnixSocket(path=self.address,
                                          ioloop=self.ioloop())
    self.server.serve()
    self._server_ssl_options = None
    self._client_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()


class MultiCallMixin(object):
  def test_simple_multicall(self):
    """Simple multi call test.
    """
    retval = 100
    args = 1
    t = 10

    def simple_method(i):
      self.assertEqual(args, i)
      return retval

    self.server.register_function(simple_method)

    def response(results):
      self.assertEqual(len(results), t)
      self.stop()

    futures = []

    for i in range(t):
      proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                             ssl_options=self._client_ssl_options)
      futures.append(proxy.simple_method(args))

    fc = FutureCollection(*futures)
    fc.all().then(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_simple_remulticall(self):
    """Simple re multi call test.
    """
    retval = 100
    args = 1
    t = 10

    def simple_method(i):
      self.assertEqual(args, i)
      return retval

    self.server.register_function(simple_method)

    def response(results):
      self.assertEqual(len(results), t)
      for result in results:
        self.assertEqual(result, retval)
      futures = []

      def response2(results2):
        self.assertEqual(len(results), t)
        for result2 in results2:
          self.assertEqual(result2, retval)
        self.stop()

      for i in range(t):
        proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                               ssl_options=self._client_ssl_options)
        futures.append(proxy.simple_method(args))

      fc = FutureCollection(*futures)
      fc.all().then(wrap(response2))

    futures = []

    for i in range(t):
      proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                             ssl_options=self._client_ssl_options)
      futures.append(proxy.simple_method(args))

    fc = FutureCollection(*futures)
    fc.all().then(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_newpool_multicall(self):
    """Simple async method call test.
    """
    logging.debug("start simple call test!")
    import datetime
    self.server.clear()
    retval = 50
    args = 1
    t = 10
    start = datetime.datetime.now()

    def simple_method(i):
      self.assertEqual(args, i)
      return retval

    self.server.register_function(simple_method)

    def response(results):
      self.assertEqual(len(results), t)
      logging.debug("total time:%s", str(datetime.datetime.now() - start))
      self.stop()

    futures = []

    def callback(proxy):
      for i in range(t):
        futures.append(proxy.simple_method(args))

      fc = FutureCollection(*futures)
      fc.all().then(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_sync_multicall(self):
    self.server.clear()
    retval = 50
    args = 1
    t = 10

    def simple_method(i):
      self.assertEqual(args, i)
      return retval

    self.server.register_function(simple_method)

    def callback():
      results = []

      for i in range(t):
        proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                               ssl_options=self._client_ssl_options)
        logging.debug("start sync")
        result = rpclib.proxy.sync(proxy.simple_method)(args)
        logging.debug("end sync result:%s", str(result))
        results.append(result)

      self.assertEqual(len(results), t)
      for result in results:
        self.assertEqual(result, retval)
      self.stop()

    Thread(target=callback).start()
    self.start(MAX_RUNNING_TIME)


@attr(species="clique", genus="fundamentals", family="rpclib", name="multicall")
class MultiCallTests(RpcTestCase, MultiCallMixin):
  def setUp(self):
    RpcTestCase.setUp(self)
    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.address = ('127.0.0.1', self.server.serve())
    self._server_ssl_options = None
    self._client_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()


@attr(species="clique", genus="fundamentals", family="rpclib", name="multiunixcall")
class MultiUnixCallTests(RpcTestCase, MultiCallMixin):
  def setUp(self):
    RpcTestCase.setUp(self)
    self.address = os.path.join(tempfile.gettempdir(), str(random.random()))
    self.server = RpcServerOverUnixSocket(path=self.address,
                                          ioloop=self.ioloop())
    self.server.serve()
    self._server_ssl_options = None
    self._client_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()

@attr(species="clique", genus="fundamentals", family="rpclib", name="websocketcall")
class WebSocketCallTests(RpcTestCase):
  def setUp(self):
    RpcTestCase.setUp(self)
    self.server = RpcServerOverWebSocket(ioloop=self.ioloop())
    self.stream = WebSocket()
    self.server.add_stream(self.stream)

  def tearDown(self):
    RpcTestCase.tearDown(self)

  def _gen_request(self, request_type, response_id, src, dst, timeout,
                   namespace, name, args, kwargs):
    output = StringIO()
    output.write("{")
    output.write("'request_type': %s," % str(request_type))
    output.write("'src': '%s'," % str(src))
    output.write("'dst': '%s'," % str(dst))
    output.write("'timeout': %s," % str(timeout))
    output.write("'namespace': '%s'," % str(namespace))
    output.write("'name': '%s'," % str(name))
    output.write("'args': [")
    first = True
    for arg in args:
      if not first:
        output.write(",")

      output.write(("'%s'" if isinstance(arg, str) else "%s") % str(arg))

      first = False
    output.write("],")

    output.write("'kwargs' :{")
    first = True
    for k, v in kwargs:
      if not first:
        output.write(",")

      output.write(("'%s': '%s'" if isinstance(v, str) else "'%s': %s") % (str(k), str(v)))
    output.write("},")

    output.write("'response_id': '%s'," % str(response_id))
    output.write("}")
    return output.getvalue()

  def test_simple_call(self):
    """Simple async method call test.
    """
    logging.debug("start simple call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    res_id = '1'

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      logging.debug("return value")
      return retval

    self.server.register_function(simple_method)

    def response(data):
      data = ast.literal_eval(data)
      self.assertEqual(data.get('result'), retval)
      self.assertEqual(data.get('response_id'), res_id)
      self.assertEqual(data.get('request_type'), SHARED_REQUEST)
      self.stop()

    request = self._gen_request(SHARED_REQUEST, res_id, '', '', 10, '',
                                'simple_method', args, dict())
    self.stream.set_write_handler(response)
    self.stream.handle_message(request)
    self.start(MAX_RUNNING_TIME)

  def test_instance_calls(self):
    """Simple async method in instance call test
    """
    logging.debug("start instance call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    testor = self
    res_id = '1'

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.server.register_instance(Test())

    def response(data):
      data = ast.literal_eval(data)
      self.assertEqual(data.get('result'), retval)
      self.assertEqual(data.get('response_id'), res_id)
      self.assertEqual(data.get('request_type'), SHARED_REQUEST)
      self.stop()

    request = self._gen_request(SHARED_REQUEST, res_id, '', '', 10, '',
                                'simple_method', args, dict())
    self.stream.set_write_handler(response)
    self.stream.handle_message(request)
    self.start(MAX_RUNNING_TIME)

  def test_namespace_call(self):
    """Simple async method call test that has namespace
    """
    logging.debug("start namespace call test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'
    res_id = '1'

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method, namespace=namespace)

    def response(data):
      data = ast.literal_eval(data)
      self.assertEqual(data.get('result'), retval)
      self.assertEqual(data.get('response_id'), res_id)
      self.assertEqual(data.get('request_type'), SHARED_REQUEST)
      self.stop()

    request = self._gen_request(SHARED_REQUEST, res_id, '', '', 10, namespace,
                                'simple_method', args, dict())
    self.stream.set_write_handler(response)
    self.stream.handle_message(request)
    self.start(MAX_RUNNING_TIME)

  def test_namespace_instance_call(self):
    """Simple async method in instance call test that has namespace
    """
    logging.debug("start name space instance test!")
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'
    testor = self
    res_id = '1'

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.server.register_instance(Test(), namespace=namespace)

    def response(data):
      data = ast.literal_eval(data)
      self.assertEqual(data.get('result'), retval)
      self.assertEqual(data.get('response_id'), res_id)
      self.assertEqual(data.get('request_type'), SHARED_REQUEST)
      self.stop()

    request = self._gen_request(SHARED_REQUEST, res_id, '', '', 10, namespace,
                                'simple_method', args, dict())
    self.stream.set_write_handler(response)
    self.stream.handle_message(request)
    self.start(MAX_RUNNING_TIME)


class MultiRoutingCallMixin(object):
  def test_multi_simple_call(self):
    """Simple async method routing call test. in routing
    """
    logging.debug("start simple routing call test")
    retval = 100
    args = [10, 'string', True]
    count = 1

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.routing_server.register_function(simple_method)

    def response(results):
      self.assertEqual(len(results), count)
      for result in results:
        self.assertEqual(result, retval)
      self.stop()

    futures = []
    proxy = SingleRpcProxy(self.address, dst=self.ident,
                           ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)

    for i in range(count):
      futures.append(proxy.simple_method(*args))

    fc = FutureCollection(*futures)
    fc.all().then(wrap(response))
    self.start(MAX_RUNNING_TIME)


@attr(species="clique", genus="fundamentals", family="rpclib", name="multi-tcptotcp-routing")
class RoutingMultiTcpToTcpCallTests(RpcTestCase, MultiRoutingCallMixin):
  """Legitimate RPC routing call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.address = ('127.0.0.1', self.server.serve())

    self.routing_server = RpcServerOverTcp(ioloop=self.ioloop())
    self.routing_address = ('127.0.0.1', self.routing_server.serve())
    self.ident = SERVER_TWO_ID

    self._client_ssl_options = None
    self._server_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    self.routing_server.stop()


@attr(species="clique", genus="fundamentals", family="rpclib", name="multi-unixtotcp-routing")
class RoutingMultiUnixToTcpCallTests(RpcTestCase, MultiRoutingCallMixin):
  """Legitimate RPC routing call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.address = os.path.join(tempfile.gettempdir(), str(random.random()))
    self.server = RpcServerOverUnixSocket(ioloop=self.ioloop(),
                                          path=self.address)
    self.server.serve()

    self.routing_server = RpcServerOverTcp(ioloop=self.ioloop())
    self.routing_address = ('127.0.0.1', self.routing_server.serve())
    self.ident = SERVER_TWO_ID

    self._client_ssl_options = None
    self._server_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    self.routing_server.stop()


def gen_routing_handler(address, ssl_options=None):
  def routing_handler(*args, **kwargs):
    with rpclib.context() as c:
      s = c.source
      return RoutingInfo(s, address, ssl_options)
  return routing_handler


class RoutingCallMixin(object):
  def test_simple_call(self):
    """Simple async method routing call test. in routing
    """
    logging.debug("start simple routing call test")
    retval = 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.routing_server.register_function(simple_method)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, dst=self.ident,
                           ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_instance_calls(self):
    """Simple async method in instance call test
       in routing
    """
    logging.debug("start instance routing call test!")
    retval = 100
    args = [10, 'string', True]
    testor = self

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.routing_server.register_instance(Test())

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, dst=self.ident,
                           ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_namespace_call(self):
    """Simple async method call test that has namespace in routing
    """
    logging.debug("start namespace routing call test!")
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method', namespace=namespace)

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.routing_server.register_function(simple_method, namespace=namespace)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, dst=self.ident,
                           namespace=namespace, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_namespace_instance_call(self):
    """Simple async method in instance call test that has namespace
       in routing
    """
    logging.debug("start name space instance routing call test!")
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'
    testor = self

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method', namespace=namespace)

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.routing_server.register_instance(Test(), namespace=namespace)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, dst=self.ident,
                           namespace=namespace, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_async_func(self):
    """Test the async remote method in routing.
    """
    logging.debug("start async routing call test!")
    retval = 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='async_method')

    def async_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def async():
        f.set_value(retval)
      self.ioloop().add_callback(async)
      return f

    self.routing_server.register_function(async_method)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, dst=self.ident,
                           ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.async_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_tuple_result_func(self):
    """Simple async method routing call test. in routing
    """
    logging.debug("start simple routing call test")
    retval = 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return (retval, retval)

    self.routing_server.register_function(simple_method)

    def response(result):
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1], retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, dst=self.ident,
                           ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_tuple_async_result_func(self):
    """Test the async remote method in routing.
    """
    logging.debug("start async routing call test!")
    retval = 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='async_method')

    def async_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def async():
        f.set_value((retval, retval))
      self.ioloop().add_callback(async)
      return f

    self.routing_server.register_function(async_method)

    def response(result):
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1], retval)
      self.stop()

    proxy = SingleRpcProxy(self.address, dst=self.ident,
                           ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    f = proxy.async_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)


class BinaryRoutingCallMixin(object):
  def test_raw_output(self):
    """Test the raw output remote method in routing.
    """
    logging.debug("start raw output test")
    data = r'010101010101' * 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='raw_method')

    def raw_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def ready_callback(binary):
        binary.write(data)

      f.set_value(Binary(len(data), ready_callback))
      return f

    self.routing_server.register_function(raw_method)

    def response(binary):
      self.assertTrue(isinstance(binary, Binary))
      self.assertEqual(binary.size, len(data))

      def check_data(received):
        self.assertEqual(received, data)

      def finish():
        binary.finish()
        self.stop()

      binary.read(check_data, finish, binary.size)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           dst=self.ident,
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_tuple_raw_output(self):
    """Test the raw output remote method in routing.
    """
    logging.debug("start tuple raw output test")
    data = r'010101010101' * 100
    args = [10, 'string', True]
    retval = 100

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='raw_method')

    def raw_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def ready_callback(binary):
        binary.write(data)

      f.set_value((retval, Binary(len(data), ready_callback)))
      return f

    self.routing_server.register_function(raw_method)

    def response(result):
      self.assertTrue(isinstance(result[1], Binary))
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1].size, len(data))

      def check_data(received):
        self.assertEqual(received, data)

      def finish():
        result[1].finish()
        self.stop()

      result[1].read(check_data, finish, result[1].size)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           dst=self.ident,
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_raw_input(self):
    """Test the remote method acception a raw input data in routing.
    """
    logging.debug("start raw input test")
    data = r'010101010101' * 100
    args = [10, 'string', True]
    retval = 100

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='raw_method')

    def raw_method(input_binary, i, s, b):
      self.assertTrue(isinstance(input_binary, Binary))
      self.assertEqual(input_binary.size, len(data))
      f = Future()

      def check_data(received):
        self.assertEqual(received, data)

      def finish():
        input_binary.finish()
        f.set_value(retval)

      input_binary.read(check_data, finish, input_binary.size)

      return f

    self.routing_server.register_function(raw_method)

    def handle_input(binary):
      binary.write(data)
      binary.finish()

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    input_binary = Binary(len(data), handle_input)
    args.insert(0, input_binary)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           dst=self.ident,
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_raw_input_output(self):
    """Test the remote method raw input and output data in routing
    """
    logging.debug("start raw input and ouput test")
    data = r'010101010101' * 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='raw_method')

    def raw_method(input_binary, i, s, b):
      self.assertTrue(isinstance(input_binary, Binary))
      self.assertEqual(input_binary.size, len(data))
      f = Future()

      def check_data(received):
        self.assertEqual(data, received)

        def ready_callback(binary):
          binary.write(received)

        output_binary = Binary(len(received), ready_callback)
        f.set_value(output_binary)

      def finish():
        input_binary.finish()

      input_binary.read(check_data, finish, input_binary.size)
      return f

    self.routing_server.register_function(raw_method)

    def handle_input(binary):
      binary.write(data)
      binary.finish()

    def handle_output(binary):
      self.assertTrue(isinstance(binary, Binary))
      self.assertEqual(binary.size, len(data))

      def check_data(received):
        self.assertEqual(received, data)

      def finish():
        binary.finish()
        self.stop()

      binary.read(check_data, finish, binary.size)

    input_binary = Binary(len(data), handle_input)
    args.insert(0, input_binary)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           dst=self.ident,
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(handle_output))
    self.start(MAX_RUNNING_TIME)

  def test_raw_output_streaming(self):
    """Test the remote method raw output streaming data in routing
    """
    logging.debug("start raw output streaming test")
    data = r'010101010101' * 1000
    chunk_size = 100  # default tornado chunk size
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='raw_method')

    def raw_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def ready_callback(binary):
        bytes_io = io.BytesIO(data)
        byte = bytes_io.read(chunk_size)
        while len(byte) > 0:
          binary.write(byte)
          byte = bytes_io.read(chunk_size)

      output = Binary(len(data), ready_callback)

      f.set_value(output)
      return f

    self.routing_server.register_function(raw_method)

    #check_count = 0
    #check_remain_chunk_size = 0
    results = StringIO()

    def handle_output(binary):
      self.assertTrue(isinstance(binary, Binary))
      self.assertEqual(binary.size, len(data))

      def streaming_callback(received):
        results.write(str(received))

      def finish_callback():
        self.assertEqual(results.getvalue(), data)
        binary.finish()
        self.stop()

      binary.read(streaming_callback, finish_callback, binary.size)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           dst=self.ident,
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(handle_output))
    self.start(MAX_RUNNING_TIME)

  def test_raw_intput_streaming(self):
    """Test the remove method raw input streaming data in routing
    """
    logging.debug("start raw input streaming test")
    data = r'010101010101' * 1000
    chunk_size = 100
    args = [10, 'string', True]
    retval = 100

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='raw_method')

    results = StringIO()

    def raw_method(input_binary, i, s, b):
      self.assertEqual(input_binary.size, len(data))
      f = Future()

      def streaming_callback(received):
        results.write(str(received))

      def finish_callback():
        self.assertEqual(results.getvalue(), data)
        f.set_value(retval)
      input_binary.read(streaming_callback, finish_callback, input_binary.size)
      return f

    self.routing_server.register_function(raw_method)

    def handle_input(binary):
      bytes_io = io.BytesIO(data)
      byte = bytes_io.read(chunk_size)
      while len(byte) > 0:
        binary.write(byte)
        byte = bytes_io.read(chunk_size)
      binary.finish()

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    input_s = Binary(len(data), handle_input)
    args.insert(0, input_s)

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           dst=self.ident,
                           ssl_options=self._client_ssl_options)
    f = proxy.raw_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)


class SharedRoutingCallMixin(object):
  def test_simple_call(self):
    """Simple async method routing call test. in routing
    """
    logging.debug("start simple routing call test")
    retval = 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.routing_server.register_function(simple_method)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_instance_calls(self):
    """Simple async method in instance call test
       in routing
    """
    logging.debug("start instance routing call test!")
    retval = 100
    args = [10, 'string', True]
    testor = self

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.routing_server.register_instance(Test())

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_namespace_call(self):
    """Simple async method call test that has namespace in routing
    """
    logging.debug("start namespace routing call test!")
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method', namespace=namespace)

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.routing_server.register_function(simple_method, namespace=namespace)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_namespace_instance_call(self):
    """Simple async method in instance call test that has namespace
       in routing
    """
    logging.debug("start name space instance routing call test!")
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'
    testor = self

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method', namespace=namespace)

    class Test(object):
      def simple_method(self, i, s, b):
        testor.assertListEqual(args, [i, s, b])
        return retval

    self.routing_server.register_instance(Test(), namespace=namespace)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_async_func(self):
    """Test the async remote method in routing.
    """
    logging.debug("start async routing call test!")
    retval = 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='async_method')

    def async_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def async():
        f.set_value(retval)
      self.ioloop().add_callback(async)
      return f

    self.routing_server.register_function(async_method)

    def response(result):
      self.assertEqual(result, retval)
      self.stop()

    def callback(proxy):
      f = proxy.async_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_tuple_result_func(self):
    """Simple async method routing call test. in routing
    """
    logging.debug("start simple routing call test")
    retval = 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return (retval, retval)

    self.routing_server.register_function(simple_method)

    def response(result):
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1], retval)
      self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_tuple_async_result_func(self):
    """Test the async remote method in routing.
    """
    logging.debug("start async routing call test!")
    retval = 100
    args = [10, 'string', True]

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='async_method')

    def async_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def async():
        f.set_value((retval, retval))
      self.ioloop().add_callback(async)
      return f

    self.routing_server.register_function(async_method)

    def response(result):
      self.assertEqual(result[0], retval)
      self.assertEqual(result[1], retval)
      self.stop()

    def callback(proxy):
      f = proxy.async_method(*args)
      f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_multi_call_single_instance(self):
    """multi call single instance.
    """
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    count = 10

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    results = Lazy()
    results.result = []

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.routing_server.register_function(simple_method)

    def response(result):
      results.result.append(result)
      self.assertEqual(result, retval)
      if len(results.result) == count:
        self.stop()

    def callback(proxy):
      for i in range(count):
        f = proxy.simple_method(*args)
        f.get_value(wrap(response))

    SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                          ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_multi_create_call(self):
    '''multi create call
    '''
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    count = 10

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    results = Lazy()
    results.result = []

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.routing_server.register_function(simple_method)

    def response(result):
      results.result.append(result)
      self.assertEqual(result, retval)
      self.assertEqual(len(SharedRpcProxy.__STREAMS__), 1)
      if len(results.result) == count:
        self.stop()

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    for i in range(count):
      SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                            ssl_options=self._client_ssl_options).then(callback)
    self.start(MAX_RUNNING_TIME)

  def test_multi_create_after_request(self):
    '''multi create call
    '''
    self.server.clear()
    retval = 100
    args = [10, 'string', True]
    count = 10

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='simple_method')

    results = Lazy()
    results.result = []

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.routing_server.register_function(simple_method)

    def response(result):
      results.result.append(result)
      self.assertEqual(result, retval)
      self.assertEqual(len(SharedRpcProxy.__STREAMS__), 1)
      if len(results.result) == count:
        self.stop()
      else:
        create()

    def create():
      SharedRpcProxy.create(self.address, ioloop=self.ioloop(),
                            ssl_options=self._client_ssl_options).then(callback)

    def callback(proxy):
      f = proxy.simple_method(*args)
      f.get_value(wrap(response))

    self.ioloop().add_callback(create)
    self.start(MAX_RUNNING_TIME)


@attr(species="clique", genus="fundamentals", family="rpclib", name="tcptotcp-ssl-routing")
class RoutineTcpToTcpSSLCallTests(RpcTestCase, RoutingCallMixin, BinaryRoutingCallMixin,
                                  SharedRoutingCallMixin):
  """Legitimate RPC routing call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self._temp_dir = tempfile.mkdtemp()
    self._root_priv_path = os.path.join(
        self._temp_dir, 'server_priv.pem')
    self._root_csr_path = os.path.join(
        self._temp_dir, 'ca.cert.csr')
    self._root_cert_path = os.path.join(
        self._temp_dir, 'ca.cert')
    self._root_key_path = os.path.join(
        self._temp_dir, 'ca.key')

    _gen_ras(self._root_priv_path)
    _gen_csr(self._root_csr_path, "/C=KO/ST=SEOUL/L=SEOUL/O=Narantech Inc/OU=Software Group/CN=CLIQUE ROOT/emailAddress=narantech@narantech.com",
             self._root_priv_path)  # gen server csr
    _gen_ca_key(self._root_priv_path, self._root_key_path)  # gen server key
    _gen_ca_cert(self._root_csr_path, self._root_key_path,
                 self._root_cert_path)  # gen server cert

    self._priv_path = os.path.join(
        self._temp_dir, 'client_priv.pem')
    self._issued_csr_path = os.path.join(
        self._temp_dir, 'c1.cert.csr')
    self._issued_cert_path = os.path.join(
        self._temp_dir, 'c1.cert')
    self._issued_key_path = os.path.join(
        self._temp_dir, 'c1.key')

    _gen_ras(self._priv_path)
    _gen_csr(self._issued_csr_path, "/C=KO/ST=SEOUL/L=SEOUL/O=Narantech Inc/OU=CLIQUE/CN=CLIQUE/emailAddress=narantech@narantech.com",
             self._priv_path)
    _gen_ca_key(self._priv_path, self._issued_key_path)
    _issue_cert(self._issued_csr_path, self._issued_cert_path,
                self._issued_key_path, self._root_cert_path,
                self._root_key_path)

    self._server_ssl_options = {
      'certfile': self._root_cert_path,
      'keyfile': self._root_key_path,
      'cert_reqs': ssl.CERT_OPTIONAL,
      'ca_certs': self._root_cert_path,
      #'ssl_version': ssl.PROTOCOL_TLSv1
    }

    self._client_ssl_options = {
      'certfile': self._issued_cert_path,
      'keyfile': self._issued_key_path,
      #'ssl_version': ssl.PROTOCOL_TLSv1
    }

    self.server = RpcServerOverTcp(ioloop=self.ioloop(),
                                   ssl_options=self._server_ssl_options)
    self.address = ("127.0.0.1", self.server.serve())

    self.routing_server = RpcServerOverTcp(ioloop=self.ioloop(),
                                           ssl_options=self._server_ssl_options)
    self.routing_address = ('127.0.0.1', self.routing_server.serve())
    self.ident = SERVER_TWO_ID

    def handle_auth(token, pub_key):
      if pub_key == open(self._client_cert_path).read():
        return True
      return False

    self.server.handle_authorization = handle_auth

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    self.routing_server.stop()
    if os.path.exists(self._temp_dir):
      shutil.rmtree(self._temp_dir)
    SharedRpcProxy.dispose_all()


@attr(species="clique", genus="fundamentals", family="rpclib", name="tcptotcp-routing")
class RoutingTcpToTcpCallTests(RpcTestCase, RoutingCallMixin, BinaryRoutingCallMixin,
                               SharedRoutingCallMixin):
  """Legitimate RPC routing call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.address = ('127.0.0.1', self.server.serve())

    self.routing_server = RpcServerOverTcp(ioloop=self.ioloop())
    self.routing_address = ('127.0.0.1', self.routing_server.serve())
    self.ident = SERVER_TWO_ID

    self._client_ssl_options = None
    self._server_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    self.routing_server.stop()
    SharedRpcProxy.dispose_all()


@attr(species="clique", genus="fundamentals", family="rpclib", name="tcptounix-routing")
class RoutingTcpToUnixCallTests(RpcTestCase, RoutingCallMixin, BinaryRoutingCallMixin,
                                SharedRoutingCallMixin):
  """Legitimate RPC routing call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.address = ('127.0.0.1', self.server.serve())

    self.path = os.path.join(tempfile.gettempdir(), str(random.random()))
    self.routing_server = RpcServerOverUnixSocket(ioloop=self.ioloop(),
                                                  path=self.path)
    self.ident = SERVER_TWO_ID
    self.routing_address = self.path
    self.routing_server.serve()

    self._client_ssl_options = None
    self._server_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    self.routing_server.stop()
    SharedRpcProxy.dispose_all()


@attr(species="clique", genus="fundamentals", family="rpclib", name="unixtounix-routing")
class RoutingUnixToUnixCallTests(RpcTestCase, RoutingCallMixin, BinaryRoutingCallMixin,
                                 SharedRoutingCallMixin):
  """Legitimate RPC routing call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.address = os.path.join(tempfile.gettempdir(), str(random.random()))
    self.server = RpcServerOverUnixSocket(ioloop=self.ioloop(),
                                          path=self.address)
    self.server.serve()

    self.path = os.path.join(tempfile.gettempdir(), str(random.random()))
    self.routing_server = RpcServerOverUnixSocket(ioloop=self.ioloop(),
                                                  path=self.path)
    self.ident = SERVER_TWO_ID
    self.routing_address = self.path
    self.routing_server.serve()

    self._client_ssl_options = None
    self._server_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    self.routing_server.stop()
    SharedRpcProxy.dispose_all()


@attr(species="clique", genus="fundamentals", family="rpclib", name="unixtotcp-routing")
class RoutingUnixToTcpCallTests(RpcTestCase, RoutingCallMixin):
  """Legitimate RPC routing call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.address = os.path.join(tempfile.gettempdir(), str(random.random()))
    self.server = RpcServerOverUnixSocket(ioloop=self.ioloop(),
                                          path=self.address)
    self.server.serve()

    self.routing_server = RpcServerOverTcp(ioloop=self.ioloop())
    self.routing_address = ('127.0.0.1', self.routing_server.serve())
    self.ident = SERVER_TWO_ID

    self._client_ssl_options = None
    self._server_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    self.routing_server.stop()
    SharedRpcProxy.dispose_all()


class RoutingStreamingCallMixin(object):
  def test_file_output_streaming(self):
    """Test streaming a file
    """
    target = 'app.target'
    target_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               target)

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='raw_method')

    def raw_method():
      def ready_callback(binary):
        FileToBinaryCopier(open(self.icon_path), binary, binary.size,
                           lambda c: None).start()

      return Future(Binary(os.path.getsize(self.icon_path),
                           ready_callback))

    self.routing_server.register_function(raw_method)

    def handle_output(binary):
      logging.debug("target path:%s", str(target_path))
      target = open(target_path, 'wb')

      def end(c):
        self.assertTrue(c.error is None)
        self.assertTrue(os.path.exists(target_path))
        self.assertTrue(os.path.exists(self.icon_path))
        with open(target_path, 'rb') as t:
          logging.debug("target size:%s, icon size:%s", str(os.path.getsize(target_path)),
                                                        str(os.path.getsize(self.icon_path)))

          self.assertEqual(os.path.getsize(target_path),
                           os.path.getsize(self.icon_path))
          with open(self.icon_path, 'rb') as a:
            self.assertEqual(t.read(), a.read())
        os.remove(target_path)
        self.stop()
      BinaryToFileCopier(binary, target, binary.size, end,
                         auto_close=True).start()

    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    proxy.raw_method().then(handle_output)
    self.start(MAX_RUNNING_TIME)

  def test_multi_files_output_streaming(self):
    """Test streaming multi files
    """
    target = 'app.target'
    target_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               target)
    count = 10

    self.server.register_function(gen_routing_handler(self.routing_address,
                                                      self._client_ssl_options),
                                  name='raw_method')

    def raw_method():
      def ready_callback(binary):
        FileToBinaryCopier(open(self.icon_path), binary, binary.size,
                           lambda c: None).start()

      return Future(Binary(os.path.getsize(self.icon_path),
                           ready_callback))

    self.routing_server.register_function(raw_method)

    def handle_output(file_path, binary):
      logging.debug("handle out target:%s", str(file_path))
      target = open(file_path, 'wb')

      def end(path, c):
        self.assertTrue(c.error is None)
        self.assertTrue(os.path.exists(path))
        self.assertTrue(os.path.exists(self.icon_path))
        logging.debug("target size:%s, icon size:%s",
                      str(os.path.getsize(path)),
                      str(os.path.getsize(self.icon_path)))
        self.assertEqual(os.path.getsize(path),
                         os.path.getsize(self.icon_path))
        with open(path, 'rb') as t:
          with open(self.icon_path, 'rb') as a:
            self.assertEqual(t.read(), a.read())
        os.remove(path)
        self.stop()
      BinaryToFileCopier(binary, target, binary.size, functools.partial(end, file_path),
                         auto_close=True).start()

    for i in range(count):
      proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop(),
                             ssl_options=self._client_ssl_options)
      proxy.raw_method().then(functools.partial(handle_output,
                                                '%s-%s' % (target_path,
                                                           str(i))))
    self.start(MAX_RUNNING_TIME)


@attr(species="clique", genus="fundamentals", family="rpclib", name="routingtcptcpstreamingcall")
class RoutingTcpToTcpStreamingCallTests(RpcTestCase, RoutingStreamingCallMixin):
  """Legitimate RPC call tests.
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.icon = 'app.ico'
    self.icon_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                  self.icon)
    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.address = ('127.0.0.1', self.server.serve())

    self.routing_server = RpcServerOverTcp(ioloop=self.ioloop())
    self.routing_address = ('127.0.0.1', self.routing_server.serve())
    self.ident = SERVER_TWO_ID

    self._client_ssl_options = None
    self._server_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    self.routing_server.stop()


@attr(species="clique", genus="fundamentals", family="rpclib", name="routingtcpunixstreamingcall")
class RoutingTcpToUnixCallTests(RpcTestCase, RoutingStreamingCallMixin):
  def setUp(self):
    RpcTestCase.setUp(self)
    self.icon = 'app.ico'
    self.icon_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                  self.icon)
    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.address = ('127.0.0.1', self.server.serve())

    self.path = os.path.join(tempfile.gettempdir(), str(random.random()))
    self.routing_server = RpcServerOverUnixSocket(ioloop=self.ioloop(),
                                                  path=self.path)
    self.ident = SERVER_TWO_ID
    self.routing_address = self.path
    self.routing_server.serve()

    self._client_ssl_options = None
    self._server_ssl_options = None

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.server.stop()
    self.routing_server.stop()


class ExceptionalMixin(object):
  def test_method_not_found_error(self):
    retval = 100
    args = [10, 'string', True]

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method)

    def response(result):
      self.assertTrue(isinstance(result, RpcMethodNotFoundError))
      self.stop()

    proxy = SingleRpcProxy(self.rpc_address, ioloop=self.ioloop())
    f = proxy.unknown_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_namespace_not_found_error(self):
    """Simple async method call test that has namespace
    """
    retval = 100
    args = [10, 'string', True]
    namespace = 'test'

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      return retval

    self.server.register_function(simple_method, namespace=namespace)

    def response(result):
      self.assertTrue(isinstance(result, NameSpaceNotFoundError))
      self.stop()

    proxy = SingleRpcProxy(self.rpc_address, ioloop=self.ioloop(),
                           namespace='unknown_namespace')
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_destination_not_found_error(self):
    args = [10, 'string', True]

    def simple_method(i, s, b):
      raise DestinationNotFoundError("destination not found")

    self.routing_server.register_function(simple_method)

    def response(result):
      self.assertTrue(isinstance(result, DestinationNotFoundError))
      self.stop()

    proxy = SingleRpcProxy(self.routing_address, ioloop=self.ioloop(),
                           dst=self.unknown_dest)
    f = proxy.simple_method(*args)
    f.get_value(wrap(response))
    self.start(MAX_RUNNING_TIME)

  def test_client_timeout(self):
    retval = 100
    args = [10, 'string', True]
    client_timeout = 1
    time_delay = 2

    def simple_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      time.sleep(time_delay)
      return retval

    self.server.register_function(simple_method)

    @wrap
    def response(result):
      self.assertTrue(isinstance(result, TimeoutError))
      self.stop()

    # client timeout occurs only in new pool
    proxy = SingleRpcProxy(self.rpc_address, ioloop=self.ioloop(),
                           timeout=client_timeout)
    f = proxy.simple_method(*args)
    f.get_value(response)
    self.start(MAX_RUNNING_TIME)

  def test_write_too_much(self):
    data = r'010101010101' * 100
    chunk_size = 100
    args = [10, 'string', True]
    retval = 100

    results = StringIO()

    def raw_method(input_binary, i, s, b):
      self.assertEqual(input_binary.size, len(data))
      f = Future()

      def streaming_callback(received):
        results.write(str(received))

      def finish_callback():
        self.assertEqual(results.getvalue(), data)
        f.set_value(retval)
        input_binary.finish()
        self.stop()
      input_binary.read(streaming_callback, finish_callback, input_binary.size)
      return f

    self.server.register_function(raw_method)

    @wrap
    def handle_input(binary):
      bytes_io = io.BytesIO(data)
      byte = bytes_io.read(chunk_size)
      byte_len = [0]
      byte_len[0] += len(byte)

      def flush():
        try:
          b = bytes_io.read(chunk_size)
          byte_len[0] += len(b)
          if len(b) > 0:
            binary.write(b, flush)
          else:
            binary.finish()
        except:
          self.set_error()
      binary.write(byte, flush)

    #response arrive early than finish writing stream.
    #the response will be called after finishing stream writing
    @wrap
    def response(result):
      self.assertEqual(result, retval)

    input_s = Binary(len(data), handle_input)
    args.insert(0, input_s)

    proxy = BinaryRpcProxy(self.rpc_address, ioloop=self.ioloop())
    f = proxy.raw_method(*args)
    f.get_value(response)
    self.start()

  def test_read_too_much(self):
    data = r'010101010101' * 100
    chunk_size = 100
    args = [10, 'string', True]

    results = StringIO()

    def raw_method(i, s, b):
      self.assertListEqual(args, [i, s, b])
      f = Future()

      def handle_output(binary):
        bytes_io = io.BytesIO(data)
        byte = bytes_io.read(chunk_size)
        byte_len = [0]
        byte_len[0] += len(byte)

        def flush():
          try:
            b = bytes_io.read(chunk_size)
            byte_len[0] += len(b)
            if len(b) > 0:
              binary.write(b, flush)
            else:
              binary.finish()
          except:
            self.set_error()
        binary.write(byte, flush)

      f.set_value(Binary(len(data), handle_output))
      return f

    self.server.register_function(raw_method)

    @wrap
    def handle_input(stream):
      def streaming_callback(received):
        results.write(str(received))

      def finish_callback():
        self.assertEqual(results.getvalue(), data)
        stream.finish()
        self.stop()
      stream.read(streaming_callback, finish_callback, stream.size)

    proxy = BinaryRpcProxy(self.rpc_address, ioloop=self.ioloop())
    proxy.raw_method(*args).then(handle_input)
    self.start()


@attr(species="clique", genus="fundamentals", family="rpclib", name="tcp-exception")
class ExceptionalTcpTests(RpcTestCase, ExceptionalMixin):
  """Exceptional RPC call tests
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.unknown_dest = "unknown"
    self.unauth_server = UnauthorizedTcpServer(ioloop=self.ioloop())
    self.unauth_address = ('127.0.0.1', self.unauth_server.serve())

    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.rpc_address = ('127.0.0.1', self.server.serve())

    self.routing_server = RpcServerOverTcp(ioloop=self.ioloop())
    self.routing_address = ('127.0.0.1', self.routing_server.serve())

    self.timeout = 1
    self.timeout_server = RpcServerOverTcp(ioloop=self.ioloop(),
                                           timeout=self.timeout)
    self.timeout_address = ('127.0.0.1', self.timeout_server.serve())

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.unauth_server.stop()
    self.server.stop()
    self.routing_server.stop()
    self.timeout_server.stop()


@attr(species="clique", genus="fundamentals", family="rpclib", name="unix-exception")
class ExceptionalUnixTests(RpcTestCase, ExceptionalMixin):
  """Exceptional RPC call tests
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self.unknown_dest = "unknown"
    self.unauth_address = os.path.join(tempfile.gettempdir(),
                                       str(random.random()))
    self.unauth_server = UnauthorizedUnixServer(path=self.unauth_address,
                                                ioloop=self.ioloop())
    self.unauth_server.serve()
    self.rpc_address = os.path.join(tempfile.gettempdir(),
                                    str(random.random()))
    self.server = RpcServerOverUnixSocket(ioloop=self.ioloop(),
                                          path=self.rpc_address)
    self.server.serve()

    self.routing_address = os.path.join(tempfile.gettempdir(),
                                        str(random.random()))
    self.routing_server = RpcServerOverUnixSocket(ioloop=self.ioloop(),
                                                  path=self.routing_address)
    self.routing_server.serve()

    self.timeout = 1
    self.timeout_address = os.path.join(tempfile.gettempdir(),
                                        str(random.random()))
    self.timeout_server = RpcServerOverUnixSocket(ioloop=self.ioloop(),
                                                  path=self.timeout_address,
                                                  timeout=self.timeout)
    self.timeout_server.serve()

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self.unauth_server.stop()
    self.server.stop()
    self.routing_server.stop()
    self.timeout_server.stop()


class UnauthorizedTcpServer(RpcServerOverTcp):
  def handle_authorization(self, token, userid):
    return False


class UnauthorizedUnixServer(RpcServerOverUnixSocket):
  def handle_authorization(self, token, userid):
    return False


@attr(species="clique", genus="fundamentals", family="rpclib", name="ssl")
class SslTests(RpcTestCase):
  """Ssl call test
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    self._temp_dir = tempfile.mkdtemp()
    self._root_priv_path = os.path.join(
        self._temp_dir, 'server_priv.pem')
    self._root_csr_path = os.path.join(
        self._temp_dir, 'ca.cert.csr')
    self._root_cert_path = os.path.join(
        self._temp_dir, 'ca.cert')
    self._root_key_path = os.path.join(
        self._temp_dir, 'ca.key')

    _gen_ras(self._root_priv_path)
    _gen_csr(self._root_csr_path, "/C=KO/ST=SEOUL/L=SEOUL/O=Narantech Inc/OU=Software Group/CN=CLIQUE ROOT/emailAddress=narantech@narantech.com",
             self._root_priv_path)  # gen server csr
    _gen_ca_key(self._root_priv_path, self._root_key_path)  # gen server key
    _gen_ca_cert(self._root_csr_path, self._root_key_path,
                 self._root_cert_path)  # gen server cert

    self._priv_path = os.path.join(
        self._temp_dir, 'client_priv.pem')
    self._issued_csr_path = os.path.join(
        self._temp_dir, 'c1.cert.csr')
    self._issued_cert_path = os.path.join(
        self._temp_dir, 'c1.cert')
    self._issued_key_path = os.path.join(
        self._temp_dir, 'c1.key')

    _gen_ras(self._priv_path)
    _gen_csr(self._issued_csr_path, "/C=KO/ST=SEOUL/L=SEOUL/O=Narantech Inc/OU=CLIQUE/CN=CLIQUE/emailAddress=narantech@narantech.com",
             self._priv_path)
    _gen_ca_key(self._priv_path, self._issued_key_path)
    _issue_cert(self._issued_csr_path, self._issued_cert_path,
                self._issued_key_path, self._root_cert_path,
                self._root_key_path)

    self._priv2_path = os.path.join(
        self._temp_dir, 'client_priv2.pem')
    self._issued2_csr_path = os.path.join(
        self._temp_dir, 'c2.cert.csr')
    self._issued2_cert_path = os.path.join(
        self._temp_dir, 'c2.cert')
    self._issued2_key_path = os.path.join(
        self._temp_dir, 'c2.key')

    _gen_ras(self._priv2_path)
    _gen_csr(self._issued2_csr_path, "/C=KO/ST=SEOUL/L=SEOUL/O=Narantech Inc/OU=CLIQUE/CN=CLIQUE/emailAddress=narantech@narantech.com",
             self._priv2_path)
    _gen_ca_key(self._priv2_path, self._issued2_key_path)
    _issue_cert(self._issued2_csr_path, self._issued2_cert_path,
                self._issued2_key_path, self._root_cert_path,
                self._root_key_path)

    self._server_ssl_options = {
      'certfile': self._root_cert_path,
      'keyfile': self._root_key_path,
      'cert_reqs': ssl.CERT_OPTIONAL,
      'ca_certs': self._root_cert_path,
      #'ssl_version': ssl.PROTOCOL_TLSv1
    }

    self._client_ssl_options = {
      'certfile': self._issued_cert_path,
      'keyfile': self._issued_key_path,
      #'ssl_version': ssl.PROTOCOL_TLSv1
    }

    self._client2_ssl_options = {
      'certfile': self._issued2_cert_path,
      'keyfile': self._issued2_key_path,
      #'ssl_version': ssl.PROTOCOL_TLSv1
    }

    self.server = RpcServerOverTcp(ioloop=self.ioloop(),
                                   ssl_options=self._server_ssl_options)
    self.address = ('127.0.0.1', self.server.serve())

  def tearDown(self):
    RpcTestCase.tearDown(self)
    if os.path.exists(self._temp_dir):
      shutil.rmtree(self._temp_dir)

  def test_public_key_receiving(self):
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def handle_stream(stream, address):
      def response(data):
        logging.debug("handle stream called")
        data = stream.socket.getpeercert(binary_form=True)
        self.assertIsNotNone(data)
        peer_cert = ssl.DER_cert_to_PEM_cert(data)
        client_cert = open(self._issued_cert_path).read()
        self.assertEqual(peer_cert, client_cert)
        self.stop()
      stream.read_bytes(10, response)

    self.server._server.handle_stream = handle_stream

    def simple_method(i, s, b):
      return retval

    self.server.register_function(simple_method)

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client_ssl_options)
    proxy.simple_method(*args)
    self.start()

  def test_appended_public_key_receiving(self):
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def handle_stream(stream, address):
      def response(data):
        logging.debug("handle stream called")
        data = stream.socket.getpeercert(binary_form=True)
        self.assertIsNotNone(data)
        peer_cert = ssl.DER_cert_to_PEM_cert(data)
        client_cert = open(self._issued2_cert_path).read()
        self.assertEqual(peer_cert, client_cert)
        self.stop()
      stream.read_bytes(10, response)

    self.server._server.handle_stream = handle_stream

    def simple_method(i, s, b):
      return retval

    self.server.register_function(simple_method)

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client2_ssl_options)
    proxy.simple_method(*args)
    self.start()

  def test_normal_ssl_call(self):
    self.server.clear()
    retval = 100
    args = [10, 'string', True]

    def simple_method(i, s, b):
      with rpclib.context() as c:
        pubkey = c.public_key
        client_cert = open(self._issued2_cert_path).read()
        self.assertEqual(pubkey, client_cert)

      return retval

    self.server.register_function(simple_method)

    def response(result):
      self.assertEqual(retval, result)
      self.stop()

    proxy = SingleRpcProxy(self.address, ioloop=self.ioloop(),
                           ssl_options=self._client2_ssl_options)
    proxy.simple_method(*args).then(response)
    self.start()


def _issue_cert(user_csr, user_cert, user_key, ca_cert, ca_key):
  _execute_cmd("x509 -req -in %s -out %s -signkey %s -CA %s -CAkey %s -CAcreateserial -days 1095" %
               (user_csr, user_cert, user_key, ca_cert, ca_key))


def _gen_csr(csr_path, subj, priv_path):
  _execute_cmd("req -passout pass:abcdefg -subj \"%s\" -key %s -new > %s" %
               (subj, priv_path, csr_path))


def _gen_ca_key(priv_path, ca_key_path):
  _execute_cmd("rsa -passin pass:abcdefg -in %s -out %s" %
               (priv_path, ca_key_path))


def _gen_ca_cert(csr_path, ca_key_path, ca_cert_path):
  _execute_cmd("x509 -in %s -out %s -req -signkey %s -days 1095" %
               (csr_path, ca_cert_path, ca_key_path))


def _gen_ras(path):
  _execute_cmd("genrsa -out %s 2048" % path)


def _execute_cmd(cmd):
  cmd_str = _make_cmd_str(cmd)
  return subprocess.check_call(cmd_str, shell=True)


def _make_cmd_str(cmd):
  logging.debug("command:%s", str(cmd))
  return 'openssl %s' % cmd


@attr(species="clique", genus="fundamentals", family="rpclib", name="binarycopier-streaming")
class BinaryCopierStreamingTests(RpcTestCase):
  def setUp(self):
    RpcTestCase.setUp(self)
    self._data = r'0101010101' * rpclib.proxy.DEFAULT_READ_CHUNK_SIZE * 10
    self._read_file = tempfile.NamedTemporaryFile(delete=False)
    self._read_path = self._read_file.name
    self._read_file.write(self._data)
    self._read_file.seek(0, 0)
    self._write_file = tempfile.NamedTemporaryFile(delete=False)
    self._write_path = self._write_file.name
    self.server = RpcServerOverTcp(ioloop=self.ioloop())
    self.address = ('127.0.0.1', self.server.serve())

  def tearDown(self):
    RpcTestCase.tearDown(self)
    if os.path.exists(self._read_path):
      os.remove(self._read_path)

    if os.path.exists(self._write_path):
      os.remove(self._write_path)

  def test_raw_intput_streaming_binary_copier(self):
    """Test the input streaming using binary copier
    """
    self.server.clear()
    retval = 100

    def raw_method(input_binary):
      self.assertEqual(input_binary.size, len(self._data))

      ft = Future()

      def finish_callback(c):
        self.assertTrue(c.error is None)
        self.assertTrue(os.path.getsize(self._write_path), input_binary.size)
        with open(self._write_path) as f:
          result = f.read()
          self.assertTrue(self._data, result)
          with open(self._read_path) as rf:
            self.assertTrue(result, rf.read())
        ft.set_value(retval)

      BinaryToFileCopier(input_binary, self._write_file,
                         input_binary.size, finish_callback,
                         auto_close=True).start()
      return ft

    self.server.register_function(raw_method)

    def callback(result):
      self.assertEqual(result, retval)
      self.stop()

    def handle_input(binary):
      FileToBinaryCopier(self._read_file, binary, binary.size,
                         lambda c: None, auto_close=True).start()

    input_s = Binary(os.path.getsize(self._read_path), handle_input)
    proxy = BinaryRpcProxy(self.address, ioloop=self.ioloop())
    proxy.raw_method(input_s).then(callback)
    self.start()

  def test_raw_output_streaming_binary_copier(self):
    """Test the output streaming using binary copier
    """
    self.server.clear()

    def raw_method():
      def ready_callback(binary):
        FileToBinaryCopier(self._read_file, binary, binary.size,
                           lambda c: None).start()

      return Future(value=Binary(os.path.getsize(self._read_path), ready_callback))

    self.server.register_function(raw_method)

    def handle_output(binary):
      self.assertTrue(isinstance(binary, Binary))
      self.assertEqual(binary.size, len(self._data))

      def finish_callback(c):
        self.assertTrue(c.error is None)
        self.assertTrue(os.path.getsize(self._write_path), binary.size)
        with open(self._write_path) as f:
          result = f.read()
          self.assertTrue(self._data, result)
          with open(self._read_path) as rf:
            self.assertTrue(result, rf.read())
        self.stop()

      BinaryToFileCopier(binary, self._write_file, binary.size,
                         finish_callback, auto_close=True).start()

    BinaryRpcProxy(self.address,
                   ioloop=self.ioloop()).raw_method().then(wrap(handle_output))
    self.start(MAX_RUNNING_TIME)


@attr(species="clique", genus="fundamentals", family="rpclib", name="binarycopier")
class BinaryCopierTests(RpcTestCase):
  def setUp(self):
    RpcTestCase.setUp(self)
    self._read_chunk_size = 100
    self._data = r'01' * rpclib.proxy.DEFAULT_READ_CHUNK_SIZE
    self._read_file = tempfile.NamedTemporaryFile(delete=True)
    self._read_path = self._read_file.name
    self._read_file.write(self._data)
    self._read_file.seek(0, 0)
    self._write_file = tempfile.NamedTemporaryFile(delete=True)
    self._write_path = self._write_file.name
    self._read_binary = Binary(len(self._data), lambda b: None)
    self._iostream = MockIOStream(self.ioloop(), data=self._data)
    self._read_stream = ReadStream(self._iostream, self.ioloop(),
                                   MockHotPotato(), len(self._data))
    self._read_binary._set_stream(self._read_stream, False)
    self._write_stream = WriteStream(self._iostream, self.ioloop(),
                                     MockHotPotato(), len(self._data))
    self._write_binary = Binary(len(self._data), lambda b: None)
    self._write_binary._set_stream(self._write_stream, False)

  def tearDown(self):
    RpcTestCase.tearDown(self)
    self._read_file.close()
    self._write_file.close()

  def test_copy_binary_to_file(self):
    def finish_callback(c):
      self.assertTrue(c.error is None)
      self._write_file.seek(0, 0)
      data = self._write_file.read()
      self.assertEqual(data, self._data)
      self.stop()

    BinaryToFileCopier(self._read_binary, self._write_file, len(self._data),
                       finish_callback).start()
    self.start()

  def test_copy_closed_binary_to_file(self):

    def handler():
      self._iostream.close()

    self._iostream.set_read_handler(handler)

    def finish_callback(c):
      self.assertTrue(c.error is not None)
      self.stop()

    def do_start():
      BinaryToFileCopier(self._read_binary, self._write_file,
                         len(self._data), finish_callback).start()
    self.ioloop().add_callback(do_start)
    self.start()

  def test_copy_file_to_binary(self):
    def finish_callback(c):
      self.assertTrue(c.error is None)
      data = self._iostream.get_data()
      self.assertEqual(data, self._data)
      self.stop()

    FileToBinaryCopier(self._read_file, self._write_binary, len(self._data),
                       finish_callback).start()
    self.start()

  def test_finish_callback_several_time_called(self):
    pass

  def test_error_occurs_on_finish_callback(self):
    pass


class MockHotPotato(object):
  def start(self):
    pass

  def stop(self):
    pass


class MockIOStream(object):
  def __init__(self, ioloop, data=None):
    if data is not None:
      self._read_stream = io.BytesIO(data)
    self._write_stream = []
    self._is_closed = False
    self._read_handler = None
    self._write_handler = None
    self._close_callback = None
    self._ioloop = ioloop

  def get_data(self):
    return ''.join(self._write_stream)

  def set_close_callback(self, handler):
    self._close_callback

  def set_read_handler(self, handler):
    self._read_handler = handler

  def set_write_handler(self, handler):
    self._write_handler = handler

  def close(self):
    self._is_closed = True
    if self._close_callback:
      self._close_callback()

  def closed(self):
    return self._is_closed

  def read_bytes(self, size, finish_callback, streaming_callback):
    if self._read_handler:
      self._read_handler()

    if self._is_closed:
      raise Exception("stream closed")

    def callback():
      logging.debug("finish read data")
      data = self._read_stream.read(size)
      finish_callback(data)

    self._ioloop.add_callback(callback)

  def write(self, data, flush_callback):
    if self._write_handler:
      self._write_handler()

    if self._is_closed:
      raise Exception("stream closed")

    def callback():
      logging.debug("append written data len:%s", str(len(data)))
      self._write_stream.append(data)
      logging.debug("flush callback called")
      flush_callback()

    self._ioloop.add_callback(callback)
