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


""" Inter-service channels module.
"""

import logging
import time
import sqlite3 as sql
import os
from cStringIO import StringIO
import functools

import clique
import rpclib
from rpclib.server import RpcServerOverUnixSocket
from rpclib.server import RoutingInfo
from rpclib.proxy import Base
from rpclib.proxy import DestinationNotFoundError, InvalidDestinationError
from rpclib.proxy import SingleRpcProxy
import clique.isc
from clique.isc import ISC_RPC_NAMESPACE, ENDPOINT_KEY_SEPARATOR
from clique.isc import Endpoint
from clique.isc import IscRegisterError, IscHandleError
from clique.isc import InvalidNodeNameError, IdentityNotFoundError
from clique.isc import AccessDeniedError, UnauthorizedError
from clique.isc import ENDPOINT_REGISTERED_TOPIC
from clique.isc import parse_location
from clique.isc import build_location
import engine.aengel
import engine.runtime
import engine.mq
from adt.concurrency import Lazy
from adt.concurrency import Future

# the data contains function with description and arg descriptions for all
# endpoints.
ENDPOINT_DATA = {}

NO_PROTECTION = clique.isc.NO_PROTECTION
LOCALONLY = clique.isc.LOCALONLY
NODEONLY = clique.isc.NODEONLY
PRIVILEGED = clique.isc.PRIVILEGED

# Engine isc runtime data
__DATA__ = Lazy()
__DATA__.add_initializer('conn', lambda: _init_db())
__DATA__.add_initializer('auth', lambda: _init_auth_db())
__DATA__.local_servers = {}  # appname -> RpcServerOverUnixSocket
__DATA__.auth_request_timeout = engine.runtime.context().engine_settings.get_int('isc.auth_request_timeout')
__DATA__.add_initializer('default_free_mode',
                         lambda: engine.runtime.context(
                         ).engine_settings.get_bool('node.free_mode', False))
__ENDPOINT_REGISTERED_TOPIC__ = None


def node_ident():
  import engine.node
  return engine.node.ident()


def _init_db():
  """Initialize endpoint database on memory and return the open cursor.
  """
  # Initialize db for endpoints on memory.
  conn = sql.connect(':memory:')
  conn.isolation_level = None
  cur = conn.cursor()
  try:
    # rev column set primary key for auto increment by sqlite3.
    cur.execute('''CREATE TABLE Local(
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                nodeid TEXT NOT NULL,
                appname TEXT NOT NULL,
                elevated INTEGER NOT NULL,
                protection INTEGER NOT NULL,
                rev INTEGER PRIMARY KEY,
                UNIQUE (namespace, name, appname, nodeid))''')
    # Create index.
    cur.execute('CREATE INDEX namespace_index on Local (namespace)')
    cur.execute('CREATE INDEX name_index on Local (name)')
    cur.execute('CREATE INDEX appname_index on Local (appname)')
    cur.execute('CREATE INDEX nodeid_index on Local (nodeid)')

    logging.debug("New local DB has been created on memory.")
  except:
    logging.exception("Failed to initialze endpoint db.")
  finally:
    cur.close()
  return conn


def _init_auth_db():
  auth_path = engine.runtime.context().rebase_to_data('.auth')
  conn = sql.connect(auth_path)
  conn.isolation_level = None
  cur = conn.cursor()
  try:
    #TODO add admin column
    cur.execute('''CREATE TABLE IF NOT EXISTS Auth(
                source TEXT NOT NULL,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                expiredate INTEGER NOT NULL,
                alloweddate INTEGER NOT NULL,
                UNIQUE (source, namespace, name))''')
    # Create index.
    cur.execute('CREATE INDEX IF NOT EXISTS source_index on Auth (source)')
    cur.execute('CREATE INDEX IF NOT EXISTS namespace_index on Auth (namespace)')
    cur.execute('CREATE INDEX IF NOT EXISTS name_index on Auth (name)')

    logging.debug("New auth DB has been created on path%s.", str(auth_path))
  except:
    logging.exception("Failed to initialze auth db.")
  finally:
    cur.close()
  return conn


def validate():
  pass


def start():
  """
  Note:
    Edits comment of start function in main.py, if edit it.
  """
  logging.info("Engine ISC started.")
  engine.runtime.engine_server().register_function(handle_public,
                                                   namespace=ISC_RPC_NAMESPACE,
                                                   name='handle')
  register_engine_endpoint(register_app_endpoint,
                           description="Register ISC service endpoint.",
                           protection=LOCALONLY)
  register_engine_endpoint(find,
                           description="Find endpoints by given name and options.$object$")
  register_engine_endpoint(dump)
  __DATA__.auth_pending_list = dict()
  __DATA__.allowed_sources = set([(auth.source,
                                   auth.namespace,
                                   auth.name) for auth in dump_auth_info()])
  global __ENDPOINT_REGISTERED_TOPIC__
  __ENDPOINT_REGISTERED_TOPIC__ = ENDPOINT_REGISTERED_TOPIC
  engine.mq.create_topic(__ENDPOINT_REGISTERED_TOPIC__)
  #TODO check expired date of allowed source


def stop():
  logging.info("Engine ISC stopped.")
  __DATA__.conn.close()
  __DATA__.auth.close()
  del __DATA__.conn
  del __DATA__.auth
  del __DATA__.auth_pending_list
  del __DATA__.allowed_sources
  engine.mq.delete_topic(ENDPOINT_REGISTERED_TOPIC)
  # TODO: remove the handle function?


def dump_auth_info():
  """Dump all endponts for testing.
  """
  cur = __DATA__.auth.cursor()
  try:
    cur.execute('SELECT * FROM Auth')
    auths = [_build_authinfo(row) for row in cur.fetchall()]
    logging.debug("Dumping all auth info. #auths=%d", len(auths))
    return auths
  finally:
    cur.close()


def dump():
  """Dump all endponts for testing.
  """
  cur = __DATA__.conn.cursor()
  try:
    cur.execute('SELECT * FROM Local')
    endpoints = [_build_endpoint(row) for row in cur.fetchall()]
    logging.debug("Dumping all endpoints. #endpoints=%d", len(endpoints))
    return endpoints
  finally:
    cur.close()


def _ensure_record_from_app(appname, ep):
  """Helper to make sure the given endpoint object is from the local app.
  """
  if ep.node_id != node_ident():
    logging.debug("Node Id mismatch! %s==%s", ep.node_id, node_ident())
    raise IscRegisterError("Node Id mismatch!")
  if not ep.appname or ep.appname != appname:
    logging.debug("Appname mismatch! %s==%s", ep.appname, appname)
    raise IscRegisterError("ISC appname mismatch!")
  if not ep.namespace or not ep.name:
    raise IscRegisterError(
        "ISC app record must have non-empty namespace and name.")


def _insert_record(ep):
  """Insert endpoint in sqlite db by given data.
  """
  cur = __DATA__.conn.cursor()
  try:
    # last NULL column for auto increment(sqlite).
    cur.execute('INSERT INTO local VALUES(?, ?, ?, ?, ?, ?, NULL)',
                (ep.namespace,
                 ep.name,
                 ep.node_id,
                 ep.appname,
                 int(ep.elevated),
                 int(ep.protection),
                 ))
    logging.debug("Inserted %s endpoint in db.", ep.name)
    if __ENDPOINT_REGISTERED_TOPIC__:
      engine.mq.publish(ENDPOINT_REGISTERED_TOPIC, ep)
  except:
    __DATA__.conn.rollback()
    logging.warn("Failed to insert %s record in db", ep.name, exc_info=True)
    raise
  finally:
    cur.close()


def _insert_auth_info(auth):
  cur = __DATA__.auth.cursor()
  try:
    cur.execute('INSERT INTO Auth VALUES(?, ?, ?, ?, ?)',
                (auth.source,
                 auth.namespace,
                 auth.name,
                 auth.expired_date,
                 auth.allowed_date
                 ))
    logging.debug("Insert auth info %s in db", str(auth))
  except:
    __DATA__.conn.rollback()
    logging.warn("Failed to insert auth info %s in db", str(auth),
                 exc_info=True)
    raise
  finally:
    cur.close()


def _remove_record(ep):
  """Remove endpoint in sqlite db by given data.
  """
  # TODO: check endpoint and raise exception.
  cur = __DATA__.conn.cursor()
  try:
    cur.execute('DELETE FROM Local WHERE namespace=? AND name=? AND '
                'nodeid=? AND appname=?',
                (ep.namespace, ep.name, ep.node_id, ep.appname))
    logging.debug("Removed %s in db.", ep.name)
  except:
    logging.exception("Failed to remove %s record, %s:%s", ep.namespace,
                      ep.name)
    __DATA__.conn.rollback()
  finally:
    cur.close()


def _remove_auth(auth):
  cur = __DATA__.auth.cursor()
  try:
    cur.execute('DELETE FROM Auth WHERE source=? AND namespace=? AND '
                'nodename=? AND appname=?',
                (auth.source, auth.namespace, auth.name))
    logging.debug("Removed auth :%s from db", str(auth))
  except:
    logging.exception("Failed to remove auth:%s record", str(auth))
    __DATA__.conn.rollback()
  finally:
    cur.close()


def _clear_app_endpoint(appname):
  """Remove app endpoint in sqlite db by given data.
  """
  cur = __DATA__.conn.cursor()
  try:
    cur.execute('DELETE FROM Local WHERE nodeid=? AND appname=?',
                (node_ident(), appname))
    logging.debug("Removed %s in db.", appname)
  except:
    logging.exception("Failed to remove all records, for %s", appname)
    __DATA__.conn.rollback()
  finally:
    cur.close()


def _build_endpoint(row):
  key = _build_key(row[0], row[1], row[3])  # key
  _, description, arg_descriptions, arg_types = ENDPOINT_DATA[key]
  ep = Endpoint(row[0],  # namespace
                row[1],  # name
                row[2],  # node_id
                appname=row[3],
                elevated=bool(row[4]),
                protection=int(row[5]),
                description=description,
                arg_descriptions=arg_descriptions,
                arg_types=arg_types
                )
  return ep


def _build_authinfo(row):
  auth = AuthInfo(row[0],  # source
                  row[1],  # namespace
                  row[2],  # name
                  row[3],  # expired date
                  row[4])  # allowed date
  return auth


def handle_public(namespace, name, *args, **kwargs):
  logging.debug("handle public!!")
  with rpclib.context() as c:
    pubkey = c.public_key
    src = c.source
    node_id, appname = parse_location(src)
    logging.debug("node_id:%s, apname:%s of source", str(node_id), str(appname))
    #check node validataion
    ident = engine.identity.find(pubkey)
    if ident:
      if ident.node_id != node_id:
        raise InvalidNodeNameError("nodename:%s is invalid with ident.name:%s",
                                   str(node_id), str(appname))
    else:
      raise IdentityNotFoundError("identity not found with pubkey:%s",
                                  str(pubkey))

    ep = _get_endpoint(namespace, name)
    logging.debug("found endpoint:%s", str(ep))
    # don't need to check ep is none
    # if ep is not found, isc handle error occurs
    if ep.protection == clique.isc.PRIVILEGED or \
            ep.protection == clique.isc.NODEONLY:
      if ep.protection == clique.isc.PRIVILEGED:
        if not engine.runtime.context().engine_policy.has_privileged(
                appname, '.'.join([namespace, name])):
          raise AccessDeniedError("Access denied, endpoint:%s is privileged. can't access on source:%s, appname:%s, namespace:%s, name:%s",
                                  str(ep), str(node_id), str(appname),
                                  str(namespace), str(name))
      elif ep.protection == clique.isc.NODEONLY:
        if appname:
          raise AccessDeniedError("Access denied, endpoint:%s is node only app:%s can't access",
                                  str(ep), str(appname))
    else:
      if ep.elevated and not __DATA__.default_free_mode:
        check_auth(src, namespace, name, *args, **kwargs)

    result = _do_handle_public(c, namespace, name, *args, **kwargs)
    logging.debug("do handle public result and return:%s", str(result))
    return result


def _do_handle_public(context, namespace, name, *args, **kwargs):
  dst = context.destination
  src = context.source
  addr = None
  ssl = None
  if dst:
    try:
      dst_node, dst_app = parse_location(dst)
    except:
      raise InvalidDestinationError("destination:%s is invalid", str(dst))
    if dst_node == node_ident():  # request for current node
        # addr for app in path
      if dst_app:
        addr = get_unix_socket_in_path(dst_app)
        # if app name is not exist, handle request in current node
    else:                               # request for different node
              # routing to other node
        addr = engine.node.get_address(dst_node)
        # ssl is needed only for public request
        ssl = engine.identity.get_ssl_options()
        if addr is None:
          raise DestinationNotFoundError("destination node:%s is not found",
                                         str(dst_node))

  if addr:
    return RoutingInfo(src, addr, ssl)

  func, ep = _get_func(namespace, name)
  if func:
    # TODO: only retrieve public APIs.
    isc_context = Lazy()
    node_id, appname = clique.isc.parse_location(src)
    isc_context.node_id = node_id
    isc_context.appname = appname
    clique.isc._set_context(isc_context)
    result = func(*args, **kwargs)
    logging.debug("result:%s of func:%s", str(result), str(func))
    return result
  else:
    raise IscHandleError("Failed to handle %s function, cannot find \
                          function." % name)


def handle_local(appname, namespace, name, *args, **kwargs):
  """Handle the local endpoint invocation.
  """
  logging.debug("handle local appname:%s, namespace:%s, name:%s",
                str(appname), str(namespace), str(name))
  with rpclib.context() as c:
    node_id = node_ident()
    current_source = clique.isc.build_location(node_id, appname)
    c.source = current_source
    dst = c.destination
    addr = None
    ssl = None
    if dst:
      try:
        dst_node, dst_app = dst.split('.')
      except:
        raise InvalidDestinationError("destination:%s is invalid", str(dst))

      if len(dst_app) > 0 and (len(dst_node) == 0 or
                               dst_node == node_id):
        # A -> N -> A
        addr = get_unix_socket_in_path(dst_app)
      elif dst_node != node_id and len(dst_node) > 0:
        # A -> N' or A -> N' -> A'
        addr = engine.node.get_address(dst_node)
        # ssl is needed only for public request
        ssl = engine.identity.get_ssl_options()
        if addr is None:
          raise DestinationNotFoundError("destination node:%s is not found",
                                         str(dst_node))
    if addr:
      logging.debug("routine from current_source:%s to target_addr:%s",
                    str(current_source), str(addr))
      return RoutingInfo(current_source, addr, ssl)

  func, ep = _get_func(namespace, name)
  if func:
    # TODO: check if appname is allowed to call the namespace.name function.
    isc_context = Lazy()
    isc_context.node_id = engine.node.ident()
    isc_context.appname = appname
    clique.isc._set_context(isc_context)
    return func(*args, **kwargs)
  else:
    raise IscHandleError("Failed to handle %s function, cannot find function." % name)


def _build_key(namespace, name, appname=''):
  return ENDPOINT_KEY_SEPARATOR.join([namespace, name, appname])


def _get_endpoint(namespace, name):
  cur = __DATA__.conn.cursor()
  # Request query for gets the function endpoint.
  try:
    # Only select the engine endpoint.
    cur.execute('SELECT * FROM Local WHERE namespace=? AND name=? AND '
                'nodeid=?', (namespace, name, node_ident()))
    # Handle row data, validate values and invoke the function.
    row = cur.fetchone()
    if not row:
      raise IscHandleError("No such ISC record.")
    ep = _build_endpoint(row)
    return ep
  finally:
    cur.close()


def _get_func(namespace, name):
  ep = _get_endpoint(namespace, name)
  if ep.appname:
    raise Exception("App Endpoint has no local function.")
  key = _build_key(namespace, name)
  # Gets function args for correct in arg descriptions.
  func, _, _, _ = ENDPOINT_DATA.get(key)
  return (func, ep)


def add_service_route(appname):
  """Add route destination in isc server when a app created by engine.
  """
  if appname and appname not in __DATA__.local_servers:
    # TODO: Register to the MQ for the app termination event.
    # create private channel
    out_path = get_unix_socket_out_path(appname)
    app_server = RpcServerOverUnixSocket(path=out_path)
    app_server.register_function(functools.partial(handle_local, appname),
                                 namespace=ISC_RPC_NAMESPACE,
                                 name='handle')
    app_server.serve()
    __DATA__.local_servers[appname] = app_server
  elif appname:
    logging.debug("service route already added. appname=%s", appname)


def remove_service_route(appname):
  """Remove the route to the service app.
  """
  if appname in __DATA__.local_servers:
    __DATA__.local_servers[appname].stop()
    del __DATA__.local_servers[appname]
    _clear_app_endpoint(appname)


def endpoint(
    namespace=None, name=None, description=None, arg_descriptions=None,
        elevated=False, protection=NO_PROTECTION):
  """ISC endpoint definition::

      @endpoint()
      def execute(appname):
        ...
    This will expose the 'execute' function through ISC so that other nodes
    can call it. Optionally, namespace and identifier can be specified.
  """
  def wrapped(f):
    register_engine_endpoint(f, namespace, name, description, arg_descriptions,
                             elevated, protection)
    return f
  return wrapped


def register_engine_endpoint(f, namespace=None, name=None, description=None,
                             arg_descriptions=None, elevated=False,
                             protection=NO_PROTECTION):
  """Registers the function on engine as an endpoint with the given data.
  """
  try:
    arg_types = clique.isc.get_func_arg_types(f)
  except Exception:
    logging.exception("Failed to register ep, cannot find function \
                      argument specs.")
  if arg_descriptions:
    for arg_key in arg_descriptions.keys():
      if arg_key not in arg_types.keys():
        raise IscRegisterError("Failed to register endpoint, cannot find %s \
                               arg in %s function arguments." % (arg_key, name))

  ep_namespace = namespace or f.__module__
  ep_name = name or f.__name__
  ep_description = description or f.__doc__ or ''

  future = Future()

  def do_register():
    # Create endpoint data and insert db and function cache.
    logging.debug("node id for endpoint :%s", str(node_ident()))
    ep = Endpoint(ep_namespace,
                  ep_name,
                  node_ident(),
                  elevated=elevated,
                  protection=protection,
                  description=ep_description,
                  arg_descriptions=arg_descriptions,
                  arg_types=arg_types)
    key = _build_key(ep_namespace, ep_name)
    _insert_record(ep)
    ENDPOINT_DATA[key] = (f, ep.description, ep.arg_descriptions, ep.arg_types)
    f.__dict__['endpoint'] = ep
    future.set_value(ep)
  clique.do(do_register)
  return future


def register_app_endpoint(ep):
  if ep:
    ep.node_id = node_ident()
    with clique.isc.context() as c:
      appname = c.appname
    _ensure_record_from_app(appname, ep)
    _insert_record(ep)
    key = _build_key(ep.namespace, ep.name, ep.appname)
    ENDPOINT_DATA[key] = (None, ep.description, ep.arg_descriptions,
                          ep.arg_types)
    return ep
  else:
    raise IscRegisterError("Failed to regiseter app endpoint, given endpoint \
                           is none.")


def find(query):
  """Find endpoints by given query.
    the query from of dictionary type.
    following defination::
      - namespace : the endpoint namespace, str
      - name : name of function, str
      - appname : name of app, str
      - protection : determine protection type, ref
      - node_id : id of node, str
      - start : offset of result endpoints, int
      - count : count of result endpoints, int
      - elevated : requiring administrative permission, bool

    and if query is empty or None, returns all registered endpoints.
  """
  # Creates query string for find endpoints by given data.
  # TODO: join with remote table.
  cur = __DATA__.conn.cursor()
  values = []
  query_str = StringIO()
  query_str.write('SELECT * FROM Local WHERE ')
  if query is not None and len(query) > 0:
    count = query.pop('count', 0)
    start = query.pop('start', 0)
    for key in query:
      query_str.write('%s=? AND ' % key)
      value = query[key]
      values.append(isinstance(value, bool) and int(value) or value)

  if len(values) <= 0:
    # no query, remove 'where' in query_str.
    query_str.seek(-6, 1)
  else:
    # has query, remove 'and' in query_str.
    query_str.seek(-4, 1)
  query_str.write("ORDER by rev")
  if query is not None and len(query) > 0:
    if count > 0:
      query_str.write(' LIMIT ?')
      values.append(count)
    if start > 0:
      query_str.write(' OFFSET ?')
      values.append(start)

  # Request db query for find endpoints.
  try:
    cur.execute(query_str.getvalue(), tuple(values))
    endpoints = [_build_endpoint(row) for row in cur.fetchall()]
    logging.debug("Found %d endpoint by given data.", len(endpoints))
    return endpoints
  except:
    logging.exception("Failed to find endpoints in db, %s")
  finally:
    query_str.close()
    cur.close()


def get_api_version():
  """Gets the current api version.
  """
  cur = __DATA__.conn.cursor()
  try:
    cur.execute('SELECT * FROM Local ORDER BY rev DESC limit 1')
    row = cur.fetchone()
    if row:
      return int(row[6])
    else:
      return 0
  except:
    logging.exception("Failed to get api version.")
  finally:
    cur.close()


def __call__(self, *args, **kwargs):
  # RpcProxy takes care of the routing this request to the appropriate
  # service application by looking at the appname as its destination ID.
  if not self.node_id:
    raise Exception("node_id must be not-empty.")
  if self.node_id == engine.node.ident() and self.appname:
    address = get_unix_socket_in_path(self.appname)
    ssl_option = None
  else:
    address = engine.node.get_address(self.node_id)
    ssl_option = engine.identity.get_ssl_options()

  if not address:
    return Future(InvalidNodeNameError("Node:%s is not in clique",
                                       str(self.node_id)))

  proxy = SingleRpcProxy(address, namespace=ISC_RPC_NAMESPACE,
                         src=build_location(node_ident(), ''),
                         dst=build_location(self.node_id, self.appname),
                         timeout=self.timeout, ssl_options=ssl_option)

  def callback(result):
    engine.node.update_requested_time(self.node_id)
    if isinstance(result, IdentityNotFoundError):
      engine.node.isolate()
    return result
  result = proxy.handle(self.namespace, self.name,
                        *args, **kwargs).then(callback)
  result.then(callback)
  return result


def get_unix_socket_in_path(appname):
  """Gets the incoming (app reads from it) unix socket path of the given
  service app.
  """
  path = os.path.join(engine.runtime.context().get_app_root_dir(appname),
                      clique.isc.INTERNAL_SOCKET_IN_PATH[1:])
  return path if not isinstance(path, unicode) else path.encode('utf-8')


def get_unix_socket_out_path(appname):
  """Gets the outgoing (app writes to it) unix socket path of the given service
  app.
  """
  path = os.path.join(engine.runtime.context().get_app_root_dir(appname),
                      clique.isc.INTERNAL_SOCKET_OUT_PATH[1:])
  return path if not isinstance(path, unicode) else path.encode('utf-8')


# Endpoint Authentication Related APIs {{{

class AuthInfo(Base):
  """Represents an endpoint authentication information.
  """
  def __init__(self, source, namespace, name, expired_date, allowed_date):
    self.source = source
    self.namespace = namespace
    self.name = name
    self.allowed_date = allowed_date
    self.expired_date = expired_date


def check_auth(source, namespace, name, *args, **kwargs):
  """Check the authentication of the source on the `namespace` and `name`.
  It returns a `token` if not authenticated yet or expired already.

  If the `token` is not authenticated AND not expired yet, then it will return
  the same non-empty `token` string. It will also return a new token if the
  given token is already invalidated.


  The 'token' format is defined as follows:

      token -> checksum timestamp
      checksum -> hmac ( source, namespace, name, timestamp )

  Hmac secret key is generated when started. The checksum is always 16 bytes
  since the underlying md5 hash algorithm is based on 128 bits.

  Note that if the token does not exist in the current node's auth DB, it
  must be retrieved from other nodes and cached for speed.
  """
  logging.debug("check auth!!")
  if (source, namespace, name) not in __DATA__.allowed_sources:
    __DATA__.auth_pending_list[(source, namespace, name)] = (args, kwargs,
                                                             int(time.time()))
    logging.debug("add auth pending list source:%s, namespace:%s, name:%s",
                  str(source), str(namespace), str(name))
    raise UnauthorizedError("source:%s, namespace:%s, name:%s is not authenticated!",
                            str(source), str(namespace), str(name))


def allow(source, namespace, name, interval):
  """Allow the token for invoking the corresponding endpoint for the specifid
  amount of `interval`.
  If `interval` is None, the token is allowed only once.
  """
  if (source, namespace, name) in __DATA__.auth_pending_list:
    args, kwargs, timestamp = __DATA__.auth_pending_list[(source, namespace, name)]
    try:
      func, ep = _get_func(namespace, name)
      func(*args, **kwargs)
      #TODO pulish function result to mq
    except:
      logging.warn("fail to recall pending request namespace:%s, name:%s",
                   str(namespace), str(name), exc_info=True)

    del __DATA__.auth_pending_list[(source, namespace, name)]

  auth = AuthInfo(source, namespace, name, int(time.time() + interval),
                  int(time.time()))
  _insert_auth_info(auth)
  __DATA__.allowed_sources.add((source, namespace, name))


def invalidate(source, namespace, name):
  """Reject/invalidate the token across all nodes.
  Note that it calls the `invalidate` function on every node because the token
  might be cached in each node.

  For the best performance, it does not delete the token from its auth DB for
  some period of time.
  """
  pass


@endpoint(protection=PRIVILEGED)
def list_pending_auth_infos(start=0, count=0, oldest_first=True):
  """List `AuthInfo`s that are not yet allowed. If `count` == 0, it returns all.
  """
  #TODO apply start and count, and order infos
  pass


@endpoint(protection=PRIVILEGED)
def list_allowed_auth_infos(start=0, count=0, oldest_first=False):
  """List `AuthInfo`s that are allowed already. If `count` == 0, it returns all.
  """
  pass


@endpoint(protection=PRIVILEGED)
def authenticate(token, ident, password):
  """Directly authenticate the `token` with the given identifier and the
  password.
  Note that only privileged applications can use this API.
  """
  pass
# }}}
