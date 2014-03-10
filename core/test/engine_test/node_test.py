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


""" node manager tests.
"""

import os
import logging
from engine.runtime import Context
from adt.config import JsonSettings
from adt.concurrency import Lazy
from adt.concurrency import Timer
from rpclib.testing import RpcTestCase
from rpclib.proxy import SingleRpcProxy
from rpclib.server import RpcServerOverTcp
from nose.plugins.attrib import attr
from engine import VERSION
from clique.isc import parse_location
import engine.identity as identity
import engine.first
import engine.runtime
import engine.node
import clique.runtime
import rpclib
import shutil
import tempfile
import ssl
from engine.node import NODE_RPC_NAMESPACE
from rpclib.stackcontext import wrap

SERVER_ONE_ID = '''server1'''


@attr(species="clique", genus="core", family="engine", name="node")
class NodeTests(RpcTestCase):
  """ test node management
  """
  def setUp(self):
    RpcTestCase.setUp(self)
    engine.first.generate_rsa_keys()
    self._temp_dir = tempfile.mkdtemp()
    self._client_ssl_options = engine.identity.get_ssl_options()
    self._db = os.path.join(self._temp_dir, '.db')
    self._handshake = os.path.join(self._temp_dir, '.hn')
    self._ident_db_path = os.path.join(self._temp_dir, '.id')

    self._server_ssl_options = {
        'certfile': engine.runtime.context().rebase_to_data(
            engine.identity.CA_CERTS_FILENAME),
        'keyfile': engine.runtime.context().rebase_to_data(
            engine.identity.__ROOT_KEY_FILENAME__),
        'cert_reqs': ssl.CERT_OPTIONAL,
        'ca_certs': engine.runtime.context().rebase_to_data(
            engine.identity.CA_CERTS_FILENAME)
    }

    engine.node.__DATA__.nodes_path = self._db
    engine.node.__DATA__.handshake_path = self._handshake
    identity.__DATA__.ident_db_path = self._ident_db_path
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'test.config')
    info_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'test.info')
    self.context = Context([info_path], [config_path])
    engine.runtime.__DATA__.context = self.context
    clique.runtime.__DATA__.context = JsonSettings([info_path])
    self.old_call = clique.isc.Endpoint.__call__
    clique.isc.Endpoint.__call__ = engine.isc.__call__
    # Initialize engie and engine status.

    engine.isc.start()
    engine.node.start()

    self._node_id = engine.node.ident()
    # FIXME: the node must not be allowed automatically.
    self._server = RpcServerOverTcp(ioloop=self.ioloop(),
                                    ssl_options=self._server_ssl_options)
    self._address = ('127.0.0.1', self._server.serve())

    self._server2 = RpcServerOverTcp(ioloop=self.ioloop(),
                                     ssl_options=self._server_ssl_options)
    self._address2 = ('127.0.0.1', self._server2.serve())
    self._old_isolate = engine.node.isolate

  def tearDown(self):
    RpcTestCase.tearDown(self)
    engine.isc.stop()
    engine.node.stop()
    engine.runtime.__DATA__.engine_server.stop()
    clique.isc.Endpoint.__call__ = self.old_call
    self._server.stop()
    self._server2.stop()
    engine.node.isolate = self._old_isolate
    if os.path.exists(self._temp_dir):
      shutil.rmtree(self._temp_dir)
    del engine.runtime.__DATA__.engine_server
    del engine.runtime.__DATA__.context
    del clique.runtime.__DATA__.context
    del engine.node.__DATA__.node_db
    del identity.__DATA__.ident_db
    del identity.__DATA__.ssl_cert_path

  def test_append_address_to_awaiting_list(self):
    src_node = "source"
    src_app = "app"
    node_id = "node"
    pubkey = "pub"
    src = src_node + '.' + src_app
    self._server.register_function(engine.node.request_handshake,
                                   namespace=NODE_RPC_NAMESPACE)

    def handshake(port, refer_node):
      self.assertTrue(self._address in engine.node.__DATA__.awaiting_list)
      self.assertEqual(refer_node, src_node)
      return (node_id, pubkey)

    self._server.register_function(handshake, namespace=NODE_RPC_NAMESPACE)

    @wrap
    def response(result):
      self.assertTrue(self._address in engine.node.__DATA__.awaiting_list)
      ident = identity.find(pubkey)
      self.assertIsNotNone(ident)
      self.assertEqual(ident.node_id, node_id)
      self.assertTrue(result)
      self.stop()

    proxy = SingleRpcProxy(self._address, src=src, ssl_options=self._client_ssl_options,
                           ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
    f = proxy.request_handshake([self._address])
    f.get_value(response)
    self.start()

  def test_request_disconnec(self):
    node_id = 'node_id'
    node_id2 = 'node_id2'

    def handle(namespace, name, *args, **kwargs):
      self.assertEqual(namespace, 'engine.node')
      self.assertEqual(name, 'disconnect')
      return True

    def callback(result):
      self.assertTrue(result)
      self.stop()

    self._server.register_function(handle)

    engine.node._insert_node_address(node_id, self._address)
    engine.node._insert_node_address(node_id2, self._address)
    engine.node.request_disconnect(node_id).then(callback)
    self.start()

  def test_remove_awaiting_list_when_handshake_failed_and_publish_message(self):
    self._src_node = "source"
    self._src_app = "app"
    self._src = self._src_node + '.' + self._src_app
    self._server.register_function(engine.node.request_handshake,
                                   namespace=NODE_RPC_NAMESPACE)
    self._mq_called = False

    def handshake(port, refer_node):
      self.assertEqual(refer_node, self._src_node)
      self.assertTrue(self._address in engine.node.__DATA__.awaiting_list)
      return Exception("fail")

    self._server.register_function(handshake, namespace=NODE_RPC_NAMESPACE)

    def mq_callback(topic, address):
      logging.debug("handshake failed mq callback topic:%s, address:%s",
                    str(topic), str(address))
      self.assertEqual(address, str(self._address))
      self.assertEqual(topic, engine.node.CONNECTION_FAILED_TOPIC)
      self._mq_called = True

    engine.mq.subscribe(engine.node.CONNECTION_FAILED_TOPIC, mq_callback)

    @wrap
    def response(result):
      self.assertFalse(self._address in engine.node.__DATA__.awaiting_list)
      self.assertTrue(result)
      self.assertTrue(self._mq_called)
      self.stop()

    proxy = SingleRpcProxy(self._address, src=self._src, ssl_options=self._client_ssl_options,
                           ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
    f = proxy.request_handshake([self._address])
    f.get_value(response)
    self.start()

  def test_handshake_free_mode(self):
    src_node = "source"
    src_app = "app"
    src = src_node + '.' + src_app
    cert_path = self._client_ssl_options['certfile']
    self._server.register_function(engine.node.handshake,
                                   namespace=NODE_RPC_NAMESPACE)

    @wrap
    def response(result):
      self.assertFalse(isinstance(result, Exception))
      node_id, pubkey = result
      self.assertFalse(engine.node.__DATA__.free_mode)
      with open(cert_path) as f:
        # the ident is registered in handshake method
        ident = identity.find(f.read())
        # check registered client pubkey and ident
        self.assertEqual(ident.node_id, src_node)

      with open(cert_path) as f:
        # check returned pubkey
        self.assertEqual(pubkey, f.read())

      self.assertEqual(node_id, self._node_id)
      self.stop()

    engine.node.__DATA__.free_mode = True
    proxy = SingleRpcProxy(self._address, src=src, ssl_options=self._client_ssl_options,
                           ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
    f = proxy.handshake(self._address[1], src_node)
    f.get_value(response)
    self.start()

  def test_handshake_refer_is_neighbor(self):
    src_node = "source"
    src_app = "app"
    src = src_node + '.' + src_app
    cert_path = self._client_ssl_options['certfile']
    self._server.register_function(engine.node.handshake,
                                   namespace=NODE_RPC_NAMESPACE)

    @wrap
    def response(result):
      node_id, pubkey = result
      self.assertEqual(node_id, self._node_id)
      with open(cert_path) as f:
        self.assertEqual(pubkey, f.read())
      self.assertFalse(engine.node.__DATA__.free_mode)
      self.assertTrue(result)

      # get client cert and check the cert registered collectly
      with open(cert_path) as f:
        ident = identity.find(f.read())
        self.assertEqual(ident.node_id, src_node)

      self.stop()

    engine.node._insert_node_address(src_node, self._address)
    proxy = SingleRpcProxy(self._address, src=src, ssl_options=self._client_ssl_options,
                           ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
    f = proxy.handshake(self._address[1], src_node)
    f.get_value(response)
    self.start()

  def test_handshake_source_is_in_clique(self):
    src_node = "source"
    src_app = "app"
    src = src_node + '.' + src_app
    cert_path = self._client_ssl_options['certfile']
    self._server.register_function(engine.node.handshake,
                                   namespace=NODE_RPC_NAMESPACE)

    def do_you_know(node_id):
      self.assertEqual(node_id, src_node)
      return True

    self._server2.register_function(do_you_know, namespace=NODE_RPC_NAMESPACE)

    @wrap
    def response(result):
      node_id, pubkey = result
      self.assertEqual(node_id, self._node_id)
      with open(cert_path) as f:
        self.assertEqual(pubkey, f.read())
      self.assertFalse(engine.node.__DATA__.free_mode)
      self.assertTrue(result)
      self.stop()

    engine.node._insert_node_address('', self._address2)
    proxy = SingleRpcProxy(self._address, src=src, ssl_options=self._client_ssl_options,
                           ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
    f = proxy.handshake(self._address[1], src_node)
    f.get_value(response)
    self.start()

  def test_let_me_join(self):
    src_node = "source"
    src_app = "app"
    src = src_node + '.' + src_app
    cert_path = self._client_ssl_options['certfile']
    self._server.register_function(engine.node.let_me_join,
                                   namespace=NODE_RPC_NAMESPACE)

    def come_in(port):
    #check node pubkey
      logging.debug("come_in called")
      with rpclib.context() as c:
        node, app = parse_location(c.source)
        with open(cert_path) as f:
          self.assertEqual(c.public_key, f.read())
        self.assertEqual(node, self._node_id)

    self._server.register_function(come_in,
                                   namespace=NODE_RPC_NAMESPACE)

    def mq_callback(topic, node_id):
      logging.debug("mq callback")
      self.assertEqual(topic, engine.node.NODE_ADDED_TOPIC)
      self.assertEqual(str(node_id), str(src_node))
      self.stop()

    engine.mq.subscribe(engine.node.NODE_ADDED_TOPIC, mq_callback)

    @wrap
    def response(result):
      #check awaiting list
      logging.debug("let me join result:%s", str(result))
      self.assertTrue(self._address not in engine.node.__DATA__.awaiting_list)
      #check requeter node is registered
      self.assertEqual(engine.node.get_address(src_node), self._address)
      self.assertTrue(result)

    engine.node.__DATA__.awaiting_list[self._address] = Lazy()
    engine.node.__DATA__.awaiting_list[self._address].timer = Timer(clique.ioloop(), 1, lambda: None)
    # insert other node to receive request handshake again
    with open(cert_path) as f:
      identity.register(src_node, f.read())
    proxy = SingleRpcProxy(self._address, src=src,
                           ssl_options=self._client_ssl_options,
                           ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
    f = proxy.let_me_join(self._address[1])
    f.get_value(response)

    self.start()

  def test_let_me_join_fail_and_publish_message(self):
    src_node = "source"
    src_app = "app"
    src = src_node + '.' + src_app
    cert_path = self._client_ssl_options['certfile']
    self._server.register_function(engine.node.handshake,
                                   namespace=NODE_RPC_NAMESPACE)
    self._server.register_function(engine.node.welcome,
                                   namespace=NODE_RPC_NAMESPACE)

    def let_me_join(port):
      with rpclib.context() as c:
        pubkey = c.public_key
        # the pubkey is node's pubkey
        with open(cert_path) as f:
          self.assertEqual(pubkey, f.read())
        self.assertTrue(os.path.exists(self._handshake))
      return Exception("fail")

    self._server.register_function(let_me_join, namespace=NODE_RPC_NAMESPACE)

    def mq_callback(topic, address):
      self.assertEqual(topic, engine.node.CONNECTION_FAILED_TOPIC)
      self.assertEqual(str(address), str(self._address))
      self.stop()

    engine.mq.subscribe(engine.node.CONNECTION_FAILED_TOPIC, mq_callback)

    @wrap
    def response(result):
      node_id, pubkey = result
      self.assertEqual(node_id, self._node_id)
      with open(cert_path) as f:
        self.assertEqual(pubkey, f.read())
      self.assertFalse(engine.node.__DATA__.free_mode)
      self.assertTrue(result)

      # get client cert and check the cert registered collectly
      with open(cert_path) as f:
        ident = identity.find(f.read())
        self.assertEqual(ident.node_id, src_node)
      proxy = SingleRpcProxy(self._address, src=src,
                             ssl_options=self._client_ssl_options,
                             ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
      proxy.welcome(self._address[1])

    engine.node._insert_node_address(src_node, self._address)
    proxy = SingleRpcProxy(self._address, src=src,
                           ssl_options=self._client_ssl_options,
                           ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
    f = proxy.handshake(self._address[1], src_node)
    f.get_value(response)
    self.start()

  def test_come_in_fail_and_publish_message(self):
    src_node = "source"
    src_app = "app"
    src = src_node + '.' + src_app
    cert_path = self._client_ssl_options['certfile']
    self._server.register_function(engine.node.let_me_join,
                                   namespace=NODE_RPC_NAMESPACE)

    def come_in(port):
      #check node pubkey
      logging.debug("come_in called!!!")
      with rpclib.context() as c:
        node, app = parse_location(c.source)
        with open(cert_path) as f:
          self.assertEqual(c.public_key, f.read())
        self.assertEqual(node, self._node_id)
      return Exception('fail')

    self._server.register_function(come_in,
                                   namespace=NODE_RPC_NAMESPACE)

    def mq_callback(topic, address):
      self.assertEqual(topic, engine.node.CONNECTION_FAILED_TOPIC)
      self.assertEqual(str(address), str(self._address))
      self.stop()

    engine.mq.subscribe(engine.node.CONNECTION_FAILED_TOPIC, mq_callback)

    @wrap
    def response(result):
      #check awaiting list
      self.assertTrue(self._address not in engine.node.__DATA__.awaiting_list)
      #check requeter node is registered
      self.assertEqual(engine.node.get_address(src_node), self._address)
      self.assertTrue(result)

    engine.node.__DATA__.awaiting_list[self._address] = Lazy()
    engine.node.__DATA__.awaiting_list[self._address].timer = Timer(clique.ioloop(), 10, lambda: None)
    # insert other node to receive request handshake again
    with open(cert_path) as f:
      identity.register(src_node, f.read())
    proxy = SingleRpcProxy(self._address, src=src,
                           ssl_options=self._client_ssl_options,
                           ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
    f = proxy.let_me_join(self._address[1])
    f.get_value(response)
    self.start()

  def test_come_in_success_and_publish_message(self):
    src_node = "source"
    src_app = "app"
    src = src_node + '.' + src_app
    cert_path = self._client_ssl_options['certfile']
    self._server.register_function(engine.node.let_me_join,
                                   namespace=NODE_RPC_NAMESPACE)

    def come_in(port):
      #check node pubkey
      logging.debug("come_in called!!!")
      with rpclib.context() as c:
        node, app = parse_location(c.source)
        with open(cert_path) as f:
          self.assertEqual(c.public_key, f.read())
        self.assertEqual(node, self._node_id)

    self._server.register_function(come_in,
                                   namespace=NODE_RPC_NAMESPACE)

    def mq_callback(topic, node_id):
      self.assertEqual(topic, engine.node.NODE_ADDED_TOPIC)
      self.assertEqual(str(node_id), str(src_node))
      self.stop()

    engine.mq.subscribe(engine.node.NODE_ADDED_TOPIC, mq_callback)

    @wrap
    def response(result):
      #check awaiting list
      self.assertTrue(self._address not in engine.node.__DATA__.awaiting_list)
      #check requeter node is registered
      self.assertEqual(engine.node.get_address(src_node), self._address)
      self.assertTrue(result)

    engine.node.__DATA__.awaiting_list[self._address] = Lazy()
    engine.node.__DATA__.awaiting_list[self._address].timer = Timer(clique.ioloop(), 10, lambda: None)
    # insert other node to receive request handshake again
    with open(cert_path) as f:
      identity.register(src_node, f.read())
    proxy = SingleRpcProxy(self._address, src=src,
                           ssl_options=self._client_ssl_options,
                           ioloop=self.ioloop(), namespace=NODE_RPC_NAMESPACE)
    f = proxy.let_me_join(self._address[1])
    f.get_value(response)
    self.start()

  def test_hello(self):
    """Tests of hello to node.
      initialize engine modules by custom context and invoke hello on status for
      return value correctly gave context values.
    """
    # Initialize node.
    version = VERSION

    def do_test():
      api_version = engine.isc.get_api_version()
      proxy = SingleRpcProxy(engine.node.get_address(self._node_id),
                             namespace=NODE_RPC_NAMESPACE,
                             ssl_options=engine.identity.get_ssl_options())
      future = proxy.hello()

      def callback(result):
        self.assertEqual(version, result.get('version'))
        self.assertIsNotNone(result.get('updated_date'))
        self.assertEqual(api_version, result.get('api_version'))
        self.assertEqual(self._node_id, result.get('node_id'))
        self.assertEqual(engine.runtime.engine_server().port,
                         result.get('port'))
        self.stop()
      # TODO: sometime cannot receive result data from server(unknown), it's a bug.
      future.get_value(wrap(callback))
    self.ioloop().add_callback(wrap(do_test))
    self.start()
