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


"""Node management on engine.
"""

import logging
import time
import random
import os

from adt.concurrency import Lazy
from adt.concurrency import Future
from adt.concurrency import Timer
from adt.concurrency import FutureCollection
from adt.funcs import try_execute
from adt.db import DoubleKeyDict
from engine.isc import endpoint
from engine.isc import PRIVILEGED
import clique
from clique.isc import parse_location
from clique.isc import build_location
from clique.isc import call_at_once
from clique.isc import IdentityNotFoundError
from clique.isc import Endpoint
from clique.isc import NODEONLY
import engine.mq
import engine.runtime
import engine.identity
import engine.progress
import engine.system
import engine.led as led
from engine import VERSION
import rpclib
from rpclib.proxy import SingleRpcProxy
from rpclib.proxy import RpcProxyCollection
import rpclib.tornado.udpstream as udpstream

# Node state
START_STATE = 1
UPGRADING_STATE = 6
RESTORING_STATE = 6

NODE_RPC_NAMESPACE = '''node_ns'''
CONNECTION_FAILED_TOPIC = '''CONNECTION_FAILED'''
CONNECTION_TIMEOUT_TOPIC = '''CONNECTION_TIMEOUT'''
# Node Topics
NODE_ADDED_TOPIC = '''node.added'''
NODE_REMOVED_TOPIC = '''node.removed'''
NODE_CONNECTED_TOPIC = '''node.connected'''
NODE_UPGRADED_TOPIC = '''node.upgraded'''
NODE_UPGRADE_FAILED_TOPIC = '''node.upgrade.failed'''

__DISCOVER_KEY__ = '''__discover__'''
__DISCOVER_REPLY_KEY__ = '''__reply__'''
__DISCOVER_DATA_SEPARATOR__ = ''':'''
__DISCOVER_INTERVAL_LIMIT__ = 10
__HANDSHAKE_INTERVAL_LIMIT__ = 60 * 5
__CHECK_HELLO_INTERVAL__ = 60 * 10
__LIMIT_COMMAND_SIZE__ = 100000

__DATA__ = Lazy()
# engine info
__DATA__.add_initializer('nodename',
                         lambda: engine.runtime.context().engine_info.get(
                             'node.name', engine.runtime.HOST_NAME))
__DATA__.add_initializer('node_id',
                         lambda: engine.runtime.context(
                         ).engine_info.get('engine.ident', _init_node_id()))
__DATA__.add_initializer('updated_date',
                         lambda: engine.runtime.context(
                         ).engine_info.get_float('engine.updated_date',
                                                 time.time()))
# node db is double key dictionary (nodename, address(ip:port), port)
__DATA__.add_initializer('node_db',
                         lambda: DoubleKeyDict(int, __DATA__.nodes_path))
__DATA__.add_initializer('localhost',
                         lambda: ('''127.0.0.1''',
                                  engine.runtime.engine_server().port))
__DATA__.add_initializer('port', lambda: engine.runtime.engine_server().port)
__DATA__.add_initializer('nodes_path',
                         lambda: engine.runtime.context(
                         ).rebase_to_data('''nodes'''))
__DATA__.add_initializer('handshake_path',
                         lambda: engine.runtime.context(
                         ).rebase_to_data('''.handshake'''))
__DATA__.add_initializer('last_accessed_times', lambda: dict())
__DATA__.add_initializer('default_free_mode',
                         lambda: engine.runtime.context(
                         ).engine_settings.get_bool('node.free_mode', False))

__DATA__.discover_key = None      # key used for discovering nodes
__DATA__.discovered_nodes = None  # addresses of the discovered nodes
__DATA__.node_state = START_STATE
__DATA__.progress_ident = ''


def validate():
  pass


def start():
  """
  Note:
    Edits comment of start function in main.py, if edit it.
  """
  # Sets a node name
  engine.runtime.context().engine_info.set('engine.name',
                                           engine.runtime.HOST_NAME)
  engine.runtime.context().engine_info.set('engine.version',
                                           engine.VERSION)

  # Register the hello function.
  # TODO: remove all unnecessary remote functions
  __DATA__.awaiting_list = dict()
  __DATA__.free_mode = False
  __DATA__.node_state = START_STATE

  # register functions related to node connection, to connect node call
  # function directly by using rpcproxy because the node to connect is not
  # registered as neighbor
  engine.runtime.engine_server(
  ).register_function(hello, namespace=NODE_RPC_NAMESPACE)
  engine.runtime.engine_server(
  ).register_function(let_me_join, namespace=NODE_RPC_NAMESPACE)
  engine.runtime.engine_server(
  ).register_function(welcome, namespace=NODE_RPC_NAMESPACE)
  engine.runtime.engine_server(
  ).register_function(come_in, namespace=NODE_RPC_NAMESPACE)
  engine.runtime.engine_server(
  ).register_function(do_you_know, namespace=NODE_RPC_NAMESPACE)
  engine.runtime.engine_server(
  ).register_function(disconnect, namespace=NODE_RPC_NAMESPACE)
  engine.runtime.engine_server(
  ).register_function(handshake, namespace=NODE_RPC_NAMESPACE)

  # Gets the default hello and discover ports.
  logging.debug("Initializing status mgmt... discover_port=%d",
                engine.runtime.DISCOVER_PORT)
  # Initialize discover server
  __DATA__.discover_port = engine.runtime.DISCOVER_PORT
  discover_server = udpstream.create_udp_stream(('', __DATA__.discover_port))
  discover_server.receive(_handle_discover)
  #TODO create handshake topic
  __DATA__.discover_server = discover_server
  _check_previous_handshake()
  __DATA__.check_hello_timer = Timer(clique.ioloop(), __CHECK_HELLO_INTERVAL__,
                                     _check_hello, repeat=True)
  engine.mq.create_topic(NODE_ADDED_TOPIC)
  engine.mq.create_topic(NODE_REMOVED_TOPIC)
  engine.mq.create_topic(NODE_CONNECTED_TOPIC)
  engine.mq.create_topic(CONNECTION_FAILED_TOPIC)
  engine.mq.create_topic(CONNECTION_TIMEOUT_TOPIC)
  engine.mq.create_topic(NODE_UPGRADED_TOPIC)
  engine.mq.create_topic(NODE_UPGRADE_FAILED_TOPIC)

  # Notify current node is connected to neighbors
  notify_connection()


def stop():
  del __DATA__.awaiting_list
  del __DATA__.free_mode
  del __DATA__.requested_time
  del __DATA__.node_id
  __DATA__.node_db.close()
  __DATA__.check_hello_timer.cancel()
  __DATA__.discover_server.close()
  del __DATA__.discover_server
  #TODO remove handshake topic
  logging.debug("Stopped engine status.")
  # should continue handshake because clique can be off when setting
  engine.mq.delete_topic(NODE_ADDED_TOPIC)
  engine.mq.delete_topic(NODE_REMOVED_TOPIC)
  engine.mq.delete_topic(CONNECTION_FAILED_TOPIC)
  engine.mq.delete_topic(CONNECTION_TIMEOUT_TOPIC)
  engine.mq.delete_topic(NODE_UPGRADED_TOPIC)
  engine.mq.delete_topic(NODE_UPGRADE_FAILED_TOPIC)
  engine.mq.delete_topic(NODE_CONNECTED_TOPIC)


def _init_node_id():
  import hashlib
  serial = engine.system.get_serial_number()
  return hashlib.md5(serial).hexdigest() if serial else serial


@endpoint()
def hello():
  # - engine version and updated time
  # - api version and updated time
  # - node name
  # - ISC port number.
  return {'version': VERSION,
          'updated_date': __DATA__.updated_date,
          'api_version': engine.isc.get_api_version(),
          'name': name(),
          'port': engine.runtime.engine_server().port,
          'node_id': ident()}


def _check_hello():
  eps = []
  current_time = int(time.time())
  for node in neighbors().keys():
    last_requested_time = __DATA__.last_accessed_times.get(node, 0)

    if current_time - last_requested_time > __CHECK_HELLO_INTERVAL__:
      eps.append((Endpoint('engine.node', 'hello', node_id=node), [], {}))

  #Don't need to isolate in callback
  #because isolate will be called in Endpoint call method
  #TODO set inactive in callback
  call_at_once(*eps).until(lambda r: isinstance(r,
                           IdentityNotFoundError))


def update_requested_time(node_id):
  __DATA__.last_accessed_times[node_id] = int(time.time())


@endpoint(elevated=True)
def upgrade(data_path, owner=None, progress_ident=None):
  """ upgrade node engine
  Args:
    data_path: a string indicating related path standard on app
    owner: a string indicating app name called this function.
    updated_file: a string indicating a compress file contains updated files.
    dels: a list indicating deleted files.
  """
  led.breathe(count=1000)
  logging.info("Upgrading a engine...")
  ident = progress_ident or engine.progress.build_ident('upgrade', 'clique')
  __DATA__.node_state = UPGRADING_STATE
  __DATA__.progress_ident = ident
  if owner:
    upgrade_path = os.path.join(engine.runtime.APPS_PATH,
                                owner, data_path[1:])
  else:
    upgrade_path = data_path

  def upgrade_cb(v):
    __DATA__.node_state = START_STATE
    led.stop()
    if isinstance(v, Exception):
      engine.mq.publish(NODE_UPGRADE_FAILED_TOPIC, __DATA__.node_id)
    else:
      engine.mq.publish(NODE_UPGRADED_TOPIC, __DATA__.node_id)
      engine.progress.update_progress(ident, 100, response=True)
    return v

  engine.runtime.worker().do(engine.aengel.upgrade, upgrade_path,
                             __DATA__.progress_ident).then(
                                 upgrade_cb)
  engine.progress.update_progress(ident, 70)
  return ident


@endpoint(elevated=True)
def restore(progress_ident=None):
  led.breathe(speed=8, count=1000)
  logging.info("Restoring a engine '%s'...". engine.VERSION)
  ident = progress_ident or engine.progress.build_ident('restore', 'clique')
  __DATA__.node_state = UPGRADING_STATE
  __DATA__.progress_ident = ident

  def restore_cb(v):
    engine.progress.update_progress(ident, 100, response=True)
    __DATA__.node_state = START_STATE
    led.stop()

  engine.runtime.worker().do(engine.aengel.restore, engine.VERSION).then(
      restore_cb)
  engine.progress.update_progress(ident, 0)
  return ident


def notify_connection():
  logging.debug("notify connection to neighbors:%s", str(neighbors()))
  eps = []
  for node_id in neighbors():
    eps.append((Endpoint('engine.node', 'handle_connection',
                         node_id=node_id), [ident()], {}))

  return call_at_once(*eps).all()


@endpoint()
def handle_connection(node_id):
  logging.debug("start handle node connection node_id:%s", str(node_id))
  with rpclib.context() as c:
    pub_key = c.public_key
    sourcenode, _ = parse_location(c.source)
    ident = engine.identity.find(pub_key)
    if ident and ident.node_id == sourcenode and ident.node_id == node_id:
      if node_id in neighbors():
        logging.debug("publish node connected event of node_id:%s",
                      str(node_id))
        engine.mq.publish(NODE_CONNECTED_TOPIC, node_id)
      else:
        logging.debug("node:%s is not in neighbors:%s",
                      str(node_id), str(neighbors()))
    else:
      logging.debug("node_id:%s, and sourcenode:%s, of ident:%s is not valified",
                    str(node_id), str(sourcenode), str(ident))


@endpoint(elevated=True)
def discover(interval=__DISCOVER_INTERVAL_LIMIT__):
  """Discover other nodes in the same network. Note that it only works for IPv4.
  It returns a `Future` object for returning the list of addresses of other
  nodes.
  """
  if __DATA__.discover_key is not None:
    logging.warn("Cancelling the on-going discover. key=%s",
                 __DATA__.discover_key)
  key = str(random.random())
  __DATA__.discover_key = key
  __DATA__.discovered_nodes = set()
  future = Future()

  def on_timeout():
    if key == __DATA__.discover_key:
      __DATA__.discover_key = None
      nodes = __DATA__.discovered_nodes
      __DATA__.discovered_nodes = None
      future.set_value(list(nodes))
    else:
      future.set_value(Exception("discover cancelled."))

  msg = __DISCOVER_DATA_SEPARATOR__.join([__DISCOVER_KEY__,
                                          key,
                                          str(time.time()),
                                          __DATA__.node_id,
                                          __DATA__.nodename,
                                          str(engine.runtime.engine_server().port)])
  udpstream.broadcast(msg, __DATA__.discover_port)
  Timer(clique.ioloop(), interval, on_timeout)
  return future


def _handle_discover(data, address):
  if data and address:
    try:
      message, ident, timestamp, node_id, nodename, port = \
          data.split(__DISCOVER_DATA_SEPARATOR__)
      if node_id == __DATA__.node_id:
        logging.debug("Discover message from the same node. Ignored.")
        return
      timestamp = float(timestamp)
      port = int(port)
      current_ts = time.time()
      #TODO(lhtak): Need to check using timestamp
      #if timestamp >= current_ts - __DISCOVER_INTERVAL_LIMIT__:
      if True:
        if message == __DISCOVER_KEY__:
          logging.debug("Replying to the discover message from %s",
                        str(address))
          logging.debug("The identity of the discover message is %s",
                        str(ident))
          reply = __DISCOVER_DATA_SEPARATOR__.join([__DISCOVER_REPLY_KEY__,
                                                    ident,
                                                    str(current_ts),
                                                    __DATA__.node_id,
                                                    __DATA__.nodename,
                                                    str(engine.runtime.engine_server().port)])
          udpstream.broadcast(reply, __DATA__.discover_port)
        elif message == __DISCOVER_REPLY_KEY__:
          if ident == __DATA__.discover_key:
            # TODO: Check if the address is already a known node.
            if node_id not in __DATA__.node_db:
              ip_and_port = address[0] + ':' + str(port)
              if ip_and_port not in __DATA__.node_db:
                __DATA__.discovered_nodes.add((node_id, nodename, address[0], port))
              else:
                logging.warn("Node %s is not a neighbor but its IP already "
                             "exists. address=%s, node=%s", node_id,
                             str(address), str(__DATA__.node_db[ip_and_port]))
            else:
              logging.debug("Node %s is already a neighbor.", node_id)
          else:
            logging.debug("Discover key mismatch. %s != %s", ident,
                          str(__DATA__.discover_key))
        else:
          logging.warn("Invalid discover key. data=%s, address=%s", data,
                       str(address))
      else:
        logging.info("Expired discover message. data=%s, address=%s", data,
                     str(address))
    except:
      logging.debug("Invalid discover message. data=%s, address=%s", data,
                    str(address), exc_info=True)


@endpoint()
def version():
  """The engine node name.
  """
  return VERSION


@endpoint()
def state():
  """The engine node state and progress ident.
  """
  return (__DATA__.node_state, __DATA__.progress_ident)


@endpoint()
def name():
  """The engine node name.
  """
  return __DATA__.nodename


@endpoint()
def ident():
  """The engine node id.
  """
  return __DATA__.node_id


@endpoint()
def get_address(node_id):
  """Gets the IP address associated with the given host name.
  """
  logging.debug("get address nodeid:%s, ident:%s",
                str(node_id), str(ident()))
  if node_id == ident():
    return __DATA__.localhost
  try:
    res = __DATA__.node_db[node_id]
    # res = (node_id, "ip:port", port)
    return (res[1].split(":")[0], res[2])
  except:
    logging.debug("fail to get address of node_id:%s", str(node_id),
                  exc_info=True)
  return None


def _insert_node_address(node_id, address):
  logging.debug("insert node:%s address:%s as neighbor",
                str(node_id), str(address))
  __DATA__.node_db[node_id, ":".join([address[0],
                   str(address[1])])] = address[1]


def _get_node_data(address):
  return __DATA__.node_db[":".join(address)]


@endpoint(elevated=True, protection=PRIVILEGED)
def rename(nodename):
  """Rename the current node. It is allowed only when no neighbors exist.
  """
  # check if it's disconnected from all other nodes
  if len(neighbors()) > 0:
    raise Exception("node must have no neighbors in order to rename.")
  # TODO: check if nodename exists in the same network
  # change the hostname
  engine.aengel.execute_cmd('hostname %s; echo %s > /etc/hostname' %
                            (nodename, nodename))

  bases = ['/']
  for appname in os.listdir(engine.runtime.APPS_PATH):
    bases.append(os.path.join(engine.runtime.APPS_PATH, appname))

  for base in bases:
    hosts_path = os.path.join(base, 'etc/hosts')
    host_cmd = 'sed -i "s/127.0.1.1\s.*/127.0.1.1\t%s/" %s' % \
        (nodename, hosts_path)
    os.path.exists(hosts_path) and engine.aengel.execute_cmd(host_cmd)
  # write to the engine configuration
  engine.runtime.context().engine_info.set('engine.name', nodename)
  engine.runtime.context().engine_info.flush()
  __DATA__.nodename = nodename


@endpoint()
def neighbors():
  """All connected neighbor nodes.
  """
  return dict([(value[0], (value[1][:value[1].rfind(":")], value[2])) \
              for value in __DATA__.node_db.values()])


@endpoint(elevated=True)
def isolate():
  """Isolate this node from all other nodes.
  """
  logging.debug("isolate node from clique")
  for node in neighbors().keys():
    try:
      del __DATA__.node_db[node]
      try_execute(engine.identity.unregister, node)
      engine.mq.publish(NODE_REMOVED_TOPIC, str(node))
    except:
      logging.warn("Error while deleting node:%s from db in isolate",
                   str(node), exc_info=True)


@endpoint(elevated=True)
def accept_all(interval):
  """Actively accept all join offers for the specified amount of `interval`.
  It's used in the FTL and possibly from others.
  """
  pass


def _is_in_clique(node_id):
  """Helper to check if the node is already in the clique network.
  """
  if len(__DATA__.node_db) > 0:
    def response(results):
      try:
        for value in results:
          if value is True:
            return True
      except:
        logging.debug("Error while handling check trusted node")
        return False
    proxy = RpcProxyCollection(neighbors().values(),
                               src=build_location(ident()),
                               ssl_options=engine.identity.get_ssl_options(),
                               namespace=NODE_RPC_NAMESPACE)
    return rpclib.proxy.greedy(proxy.do_you_know,
                               lambda r: r is True)(node_id).then(response)
  else:
    return Future(value=False)


@endpoint(elevated=True)
def set_default_free_mode(is_turn_on):
  """ Sets free mode default value
  """
  engine.runtime.context().engine_settings.set('node.free_mode',
                                               int(bool(is_turn_on)))
  engine.runtime.context().engine_settings.flush()


@endpoint(elevated=True)
def activate_free_mode(interval, accept_list=None):
  """Turn on the free mode so it can accept any join requests.
  In free mode, the following APIs are available with no authentication.
  * handshake
  * rename

  `accept_list` is a list of node names to accept. If given, it will only
  accept the corresponding nodes when requested for join.
  """
  __DATA__.free_mode = True

  def finish():
    __DATA__.free_mode = False

  Timer(clique.ioloop(), interval, finish)


def is_free_mode():
  """Check if node is in free mode
  """
  return __DATA__.free_mode


@endpoint(elevated=True)
def request_dismiss():
  """Dismiss all nodes in clique.
  """
  led.breathe(count=3)
  logging.debug("start dismiss")
  eps = []

  for node in neighbors().keys():
    eps.append((Endpoint('engine.node', 'isolate', node_id=node),
                [], {}))

  def callback(results):
    isolate()

    return results is not None and not isinstance(results, Exception)

  return call_at_once(*eps).all().then(callback)


@endpoint(elevated=True)
def request_disconnect(node_ids):
  """ disconnect node from clique network.
  if disconnection is success, return True or False
  if node name is current node's name, isolate node from clique,
  and request disconnect current node to it's neighbors
  """
  led.breathe(count=3)
  logging.debug("request disconnect node_ids:%s", str(node_ids))
  eps = []

  for node in neighbors().keys():
    eps.append((Endpoint('engine.node', 'disconnect', node_id=node),
                [node_ids], {}))

  def callback(results):
    try:
      for node_id in node_ids:
        if node_id != ident():
          if node_id in __DATA__.node_db:
            del __DATA__.node_db[node_id]
            try_execute(engine.identity.unregister, node_id)
            engine.mq.publish(NODE_REMOVED_TOPIC, str(node_id))
        else:
          # isolate should be called lately than disconnect, because disconnect use
          # neighbors' address
          isolate()
    except:
      logging.warn("Fail to remove node:%s from node db",
                   exc_info=True)
      return False

    return results is not None and not isinstance(results, Exception)

  return call_at_once(*eps).all().then(callback)


@endpoint(protection=NODEONLY)
def disconnect(node_ids):
  logging.debug("disconnect node_ids:%s", str(node_ids))
  led.breathe(count=3)
  try:
    for node_id in node_ids:
      if node_id != ident():
        del __DATA__.node_db[node_id]
        try_execute(engine.identity.unregister, node_id)
        engine.mq.publish(NODE_REMOVED_TOPIC, str(node_id))
      else:
        # if disconnected nodename is same, isolate the current node from clique.
        isolate()
    return True
  except:
    logging.warn("Fail to disconnect node:%s", str(node_ids),
                 exc_info=True)

  return False


def _remove_context(address):
  try:
    context = __DATA__.awaiting_list.get(address)
    if context:
      context.timer.cancel()
      try_execute(__DATA__.awaiting_list.__delitem__, address)
      try_execute(engine.identity.unregister, context.node_id)
  except:
    logging.debug("Fail to remove context on address:%s",
                  str(address), exc_info=True)


def _do_request_handshake(address, referrer):

  def callback(result):
    logging.debug("do hanshake callback!! result:%s", str(result))
    res = result is not None and not isinstance(result, Exception) and not False
    if not res:
      context = __DATA__.awaiting_list.get(address)
      if context:
        context.timer.cancel()
        try_execute(__DATA__.awaiting_list.__delitem__, address)
      engine.mq.publish(CONNECTION_FAILED_TOPIC, str(address))
    else:
      node_id, pubkey = result
      engine.identity.register(node_id, pubkey)
      # add nodename to clear target info
      context = __DATA__.awaiting_list.get(address)
      if context:
        context.node_id = node_id

      SingleRpcProxy(
          address, namespace=NODE_RPC_NAMESPACE,
          src=build_location(ident()),
          ssl_options=engine.identity.get_ssl_options()).welcome(__DATA__.port)
    return res

  def on_timeout():
    _remove_context(address)
    engine.mq.publish(CONNECTION_TIMEOUT_TOPIC, str(address))

  # in context following valus can be set
  # timer for timeout, handshake target address, target nodename and pubkey
  context = Lazy()
  context.timer = Timer(clique.ioloop(), __HANDSHAKE_INTERVAL_LIMIT__,
                        on_timeout)
  context.address = address
  __DATA__.awaiting_list[address] = context
  logging.debug("call handshake to address:%s", str(address))

  return SingleRpcProxy(
      address, namespace=NODE_RPC_NAMESPACE,
      src=build_location(ident()),
      ssl_options=engine.identity.get_ssl_options()).handshake(
          __DATA__.port, referrer).then(callback)


@endpoint(elevated=True)
def request_handshake(addresses):
  """Call the handshake remote function on the given address.
  It must call the function without its SSL certificate.
  """
  #TODO don't need to wait handshake callback, the error will be published through mq
  logging.debug("request handshake with addresses:%s",
                str(addresses))
  led.breathe(count=3)
  with rpclib.context() as c:
    logging.debug("request_handshake source:%s", str(c.source))
    referrer, app = parse_location(c.source)

    futures = []
    for address in addresses:
      address = tuple(address)
      if ":".join([address[0], str(address[1])]) not in __DATA__.node_db:
        if address not in __DATA__.awaiting_list:
          futures.append(_do_request_handshake(address, referrer))
        else:
          logging.debug("address :%s is already in awaiting list",
                        str(address))
      else:
        logging.debug("address :%s is already in node db", str(address))

    def callback(results):
      return results is not None and not isinstance(results, Exception)

  return FutureCollection(*futures).all().then(callback)


def handshake(port, referrer):
  """Handshake with the node. It essentially exchanges publick keys with each
  other so they can communicate.
  It accepts a public key of the peer node and gives back its public key in
  return.
  This is not an endpoint, and it is only enabled in *free mode*.
  """
  logging.debug("handshake called!! port:%s, referrer:%s",
                str(port), str(referrer))
  led.breathe(count=3)
  with rpclib.context() as c:
    client_address = (c.client_address[0], port)
    source, app = parse_location(c.source)
    pub_key = c.public_key

    # should check client address is in awaiting list
    # because in re request handshake can be called in same time in each node
    # when request handle shake with multi addresses
    if client_address not in __DATA__.awaiting_list:
      engine.identity.register(source, pub_key)
      with open(__DATA__.handshake_path, 'w') as h:
        # write address info of handshake requester
        logging.debug("write handshake file info - address:%s, port:%s, nodename:%s",
                      str(client_address[0]), str(port), str(source))
        h.write(':'.join([client_address[0], str(port), source]))

      def check():
        logging.debug("start check!!! is free mode:%s, data free mode:%s",
                      str(is_free_mode()), str(__DATA__.default_free_mode))
        if is_free_mode() or __DATA__.default_free_mode:
          # clique should be set on free mode, because clique can be turn off by
          # setting
          try:
            logging.debug("the node is free mode and start setting")
            _setting()
          except Exception, e:
            logging.warn("fail to setting on node:%s",
                         str(name()), exc_info=True)
            # if setting is fail, remove hand shake file
            try_execute(_remove_handshake_file)
            try_execute(engine.identity.unregister, source)
            return Future(value=e)

          __DATA__.free_mode = False
          return Future(value=(ident(), engine.identity.get_cert(raw=True)))
        else:
          #result if refer node is trust node dont need to check trust node
          if referrer and referrer in neighbors():
            return Future(value=(ident(), engine.identity.get_cert(raw=True)))
          else:
            def callback(result):
              # if source that request handshake
              logging.debug("is in clique result:%s", str(result))
              if result:
                return (ident(), engine.identity.get_cert(raw=True))
              else:
                # if the source is not trusted in clique not in free mode, fail
                # hanshake and remove handshake file
                try_execute(_remove_handshake_file)
                try_execute(engine.identity.unregister, source)
                return Exception("the source %s is not in clique" % str(source))
            logging.debug("reffer is not neighbor check the source:%s is in clique",
                          str(source))
            return _is_in_clique(source).then(callback)

      def handle(result):
        return result

      return check().then(handle)
    else:
      logging.debug("the client address:%s already in awaiting list",
                    str(client_address))
      return Exception("the client address:%s already in awaiting list" %
                       str(client_address))


def welcome(port):
  """Called by node only.
  Notify to this node that you can let me join.
  """
  led.breathe(count=3)
  with rpclib.context() as c:
    client_address = (c.client_address[0], port)
    node_id, _ = parse_location(c.source)
    pub_key = c.public_key
    logging.debug("welcome called from client address:%s",
                  str(client_address))

    ident = engine.identity.find(pub_key)
    if ident and ident.node_id == node_id:
      clique.do(_check_previous_handshake)
      return True
    else:
      logging.debug("Fail to welcome from client address:%s \
              node_id:%s is not exist in identity",
                    str(client_address), str(node_id))
      return False


def _remove_handshake_file():
  if os.path.exists(__DATA__.handshake_path):
    os.remove(__DATA__.handshake_path)


def _check_previous_handshake():
  path = __DATA__.handshake_path
  if os.path.exists(path):
    try:
      with open(path) as f:
        raw = f.read().split(':')
        client_address = (raw[0], int(raw[1]))

        node_id = raw[2]
        logging.debug("check previous handshake client_address:%s",
                      str(client_address))
    except:
      logging.exception("Error while read client address for handshaking")
      os.remove(__DATA__.handshake_path)
      return

    def callback(result):
      if result is None or isinstance(result, Exception) or not result:
        try_execute(engine.identity.unregister, node_id)
        engine.mq.publish(CONNECTION_FAILED_TOPIC, str(client_address))
      else:
        try_execute(_remove_handshake_file)

    def do_let_me_join():
      logging.debug('call let me join to address:%s', str(client_address))
      proxy = SingleRpcProxy(client_address, namespace=NODE_RPC_NAMESPACE,
                             src=build_location(ident()),
                             ssl_options=engine.identity.get_ssl_options())
      proxy.let_me_join(engine.runtime.engine_server().port).then(callback)
    #Should call let me join in ioloop callback
    #Because public key should be returned to request node before calling let me join
    #the pubkey is checked in let me join
    clique.do(do_let_me_join)
  else:
    logging.debug("previous handshake path:%s not exists",
                  str(path))


def let_me_join(port):
  """Called by node only.
  Asks this node for it to join the clique network.
  """
  led.breathe(count=3)
  with rpclib.context() as c:
    client_address = (c.client_address[0], port)
    node_id, app = parse_location(c.source)
    pub_key = c.public_key
    logging.debug("let me join called from client address:%s",
                  str(client_address))
    # let me join source should in pending list
    if client_address in __DATA__.awaiting_list:
      node_ident = engine.identity.find(pub_key)
      if node_ident and node_ident.node_id == node_id:
        # the ident registered in handshake
        logging.debug("insert node address name:%s, address:%s",
                      str(node_id), str(client_address))
        _insert_node_address(node_id, client_address)

        #timeout cancel and remove context
        context = __DATA__.awaiting_list.get(client_address)
        if context:
          context.timer.cancel()
          try_execute(__DATA__.awaiting_list.__delitem__, client_address)

        # this callback will be called when come in finished
        def callback(result):
          # if come in failed, remove node info
          if isinstance(result, Exception):
            #handshake failed publish message
            logging.debug("callback in error")
            engine.mq.publish(CONNECTION_FAILED_TOPIC, str(client_address))
            try_execute(__DATA__.node_db.__delitem__, node_id)
            try_execute(engine.identity.unregister, node_id)
          else:
            logging.debug("complete node connection %s:%s",
                          str(ident()), str(node_id))
            engine.mq.publish(NODE_ADDED_TOPIC, node_id)

        _allow_join(node_id, client_address).then(callback)
        # address should be removed from awaiting list
        # in let me join callback to avoid that
        # let me join called several times
        return True
      else:
        logging.debug("Fail to let me join from client address:%s, \
                node_id:%s is not exist in identity",
                      str(client_address), str(node_id))
        return False
    else:
      logging.debug("Fail to let me join client address:%s not in awaiting list",
                    str(client_address))
      return False


def _allow_join(node_id, target_address):
  """Allowed the given node to join the clique.
  """
  def comein_callback(result):
    # if come success, remove node_id from request pending list and
    # notify to other cliques that new node added
    if not isinstance(result, Exception):
      # notify to other clique to handshake
      if len(neighbors()) > 0:
        logging.debug("rerequest_handshake to target_address:%s to neighbors:%s",
                      str(target_address), str(neighbors().values()))

        def callback(result):
          logging.debug("rerequest_handshake result:%s", str(result))

        new_neighbors = []
        target_ip, _ = target_address
        #neighbor address is tuple ('ip', port)
        #to exclude neighbor same as target_address
        for neighbor_address in neighbors().values():
          neighbor_ip, _ = neighbor_address
          if neighbor_ip != target_ip:
            new_neighbors.append(neighbor_address)

        if new_neighbors:
          request_handshake_ep = Endpoint('engine.node', 'request_handshake',
                                          node_id=node_id)
          request_handshake_ep(new_neighbors).then(callback)
        else:
          logging.debug("no neighbors to rerequest_handshake")
    else:
      try_execute(__DATA__.node_db.__delitem__, node_id)
      try_execute(engine.identity.unregister, node_id)
    return result

  logging.debug("call come in in address:%s", str(target_address))
  proxy = SingleRpcProxy(target_address, namespace=NODE_RPC_NAMESPACE,
                         src=build_location(ident()),
                         ssl_options=engine.identity.get_ssl_options())
  return proxy.come_in(engine.runtime.engine_server().port).then(
      comein_callback)


def come_in(port):
  """Called by node only.
  Allow this node to join the clique network.
  """
  led.breathe(count=3)
  with rpclib.context() as c:
    address = (c.client_address[0], port)
    node_id, app = parse_location(c.source)

    if node_id not in neighbors():
      _insert_node_address(node_id, address)
      logging.debug("complete node connection %s:%s",
                    str(name()), str(node_id))
      engine.mq.publish(NODE_ADDED_TOPIC, node_id)
    else:
      logging.debug("node:%s is already neighbor to node:%s",
                    str(node_id), str(ident()))


def can_trusted(node_id):
  """ if the node is trusted, the node can call node only api
  """
  return len(__DATA__.node_db) > 0 or node_id in __DATA__.awaiting_list


def do_you_know(node_id):
  """Called by node only.
  Check if the given node locates adjacently.
  """
  return node_id in __DATA__.node_db


def _setting():
  pass
