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


"""ISC library.

  E.g.::

    - async(default)
    from clique.isc import find
    def result_callback(webservers):
      map(lambda f: f.register_handler(handler), webservers)

    webservers = find('webserver', callback=result_callback)
"""

import logging
import inspect

import clique
import rpclib
from rpclib.proxy import SingleRpcProxy
from rpclib.proxy import SharedRpcProxy
from rpclib.proxy import BinaryRpcProxy
from rpclib.proxy import Base
from rpclib.proxy import Future
from rpclib.server import RpcServerOverUnixSocket
from rpclib.proxy import RpcError, _RpcError
from contextlib import contextmanager
from functools import partial
import threading
import clique.runtime

from adt.concurrency import Lazy
from adt.concurrency import FutureCollection


# Endpoint Protection Types
NO_PROTECTION = 0   # no protection on the endpoint.
LOCALONLY = 1       # only local apps are allowed.
NODEONLY = 2        # only calls from other engines are allowed.
PRIVILEGED = 3      # only specially designed apps are allowed.

# Authentication methods
AUTH_METHOD_WEB = 1
AUTH_METHOD_EMAIL = 2
AUTH_METHOD_DIRECT = 3

ENDPOINT_REGISTERED_TOPIC = '''endpoint.registered'''
# Function map.
# TODO: unregist record and func map.
FUNC_MAP = {}
# Auth token.
TOKEN = None
# Internal incoming unix socket path for communicating with the engine.
INTERNAL_SOCKET_IN_PATH = '''/data/_socket_in'''
# Internal outgoing unix socket path for communicating with the engine.
INTERNAL_SOCKET_OUT_PATH = '''/data/_socket_out'''
# ISC RPC namespace
ISC_RPC_NAMESPACE = '''isc'''
# Separator used for the endpoint map.
ENDPOINT_KEY_SEPARATOR = ''':'''
# ISC default timeout
DEFAULT_TIMEOUT = 30
# Endpoint contexts
__CONTEXTS__ = threading.local()

# ISC runtime data
__DATA__ = Lazy()


def start():
  """Start an ISC server on app.
  """
  app_server = RpcServerOverUnixSocket(path=INTERNAL_SOCKET_IN_PATH)
  app_server.register_function(handle, namespace=ISC_RPC_NAMESPACE)
  app_server.serve()
  __DATA__.app_server = app_server


def stop():
  __DATA__.app_server.stop()
  del __DATA__.app_server


def find(query):
  """Find endpoints with the given name asynchronously (returning the `Future`)
    the query from of dictionary type.

    Following defination::

      - namespace : the endpoint namespace, str
      - name : name of function, str
      - appname : name of app, str
      - elevated : determine private endpoint, bool
      - node_id : id of node, str
      - start : offset of result endpoints(default:0), int
      - count : count of result endpoints(default:unlimit), int
      - elevated : requiring administrative permission, bool
                      and if query is empty or None, returns all registered endpoints.
  """
  ep = Endpoint('engine.isc', 'find')
  return ep(query)


def find_sync(query):
  """Helper to call the `find` function synchronously.
  """
  ep = Endpoint('engine.isc', 'find')
  return ep.sync(query)


class AllEndpoints(Base):
  """Endpoint for all nodes include current node endpoint
  """
  def __init__(self, namespace, name, appname='', timeout=DEFAULT_TIMEOUT,
               include_self=True, binary=False):
    self.namespace = namespace
    self.name = name
    self.appname = appname
    self.timeout = timeout
    self.include_self = include_self
    self.binary = binary

  def __call__(self, *args, **kwargs):
    def call(nodes):
      eps = []
      for node_id in nodes:
        eps.append((Endpoint(self.namespace, self.name, node_id=node_id,
                             appname=self.appname, timeout=self.timeout,
                             binary=self.binary), args, kwargs))
      return call_at_once(*eps).all()

    def callback(nodes, node):
      if node is not None and not isinstance(node, Exception):
        nodes.append(node)
        return call(nodes)
      else:
        logging.debug("Fail to get current node_id:%s", str(node))
        return node

    def node_ids(neighbors):
      if neighbors is not None and not isinstance(neighbors, Exception):
        if self.include_self:
          return Endpoint("engine.node",
                          "ident")().then(partial(callback, neighbors.keys()))
        else:
          return call(neighbors.keys())
      else:
        logging.debug("Fail to call all endpoint namespace:%s, name:%s,\
                      appname:%s, result:%s", str(self.namespace),
                      str(self.name), str(self.appname), str(neighbors))
        return neighbors

    return Endpoint('engine.node', 'neighbors')().then(node_ids)


class Endpoint(Base):
  """Represents an ISC endpoint from engine, app.
  """
  def __init__(self, namespace, name, node_id='', appname='',
               elevated=False, protection=NO_PROTECTION, description='',
               arg_descriptions=None, arg_types=None, timeout=DEFAULT_TIMEOUT,
               binary=False):
    self.namespace = namespace
    self.name = name
    self.node_id = node_id
    self.appname = appname
    self.elevated = elevated
    self.protection = protection
    self.description = description
    self.arg_descriptions = arg_descriptions
    self.arg_types = arg_types
    self.timeout = timeout
    self.binary = binary

  def __call__(self, *args, **kwargs):
    # RpcProxy takes care of the routing this request to the appropriate
    # service application by looking at the appname as its destination ID.
    if self.binary:
      proxy = BinaryRpcProxy(INTERNAL_SOCKET_OUT_PATH, ISC_RPC_NAMESPACE, '',
                             build_location(self.node_id, self.appname),
                             timeout=self.timeout)
      return proxy.handle(self.namespace, self.name, *args, **kwargs)
    else:
      def callback(proxy):
        return proxy.handle(self.namespace, self.name, *args, **kwargs)
      return SharedRpcProxy.create(INTERNAL_SOCKET_OUT_PATH, ISC_RPC_NAMESPACE,
                                   '', build_location(self.node_id,
                                                      self.appname),
                                   timeout=self.timeout).then(callback)

  def __repr__(self):
    return 'Endpoint(' + ','.join([str('namespace:' + self.namespace),
                                   str('name:' + self.name),
                                   str('node_id:' + self.node_id),
                                   str('appname:' + self.appname)]) + ')'

  @property
  def sync(self):
    return SyncEndpoint(self)

  def request_auth(self, method=AUTH_METHOD_WEB, **kwags):
    """Request for authentication of this endpoint.

    `method` == AUTH_METHOD_WEB
      It returns a future containing an URL to ask the user to authenticate
      the API.

    `method` == AUTH_METHOD_EMAIL
      It returns a future containing a boolean value indicating whether the
      email has sent successfully.

    `method` == AUTH_METHOD_DIRECT
      It returns a future containing a boolean value indicating authentication
      result.
      Although it is normally used by privileged applications, non-privileged
      applications can also use this `method` to sliently request for
      authentication (admins can see all auth requests in the admin panel).

    The topic token can be used as an auth token, and it must be provided when
    invoking the endpoint in the future again.
    """
    pass


class SyncEndpoint(Base):
  """Represents a synchronous version of `Endpoint`.
  """
  def __init__(self, ep):
    self.ep = ep

  def __call__(self, *args, **kwargs):
    if self.ep.binary:
      proxy = BinaryRpcProxy(INTERNAL_SOCKET_OUT_PATH, ISC_RPC_NAMESPACE, '',
                             build_location(self.ep.node_id, self.ep.appname))
    else:
      proxy = SingleRpcProxy(INTERNAL_SOCKET_OUT_PATH, ISC_RPC_NAMESPACE, '',
                             build_location(self.ep.node_id, self.ep.appname))

    return rpclib.proxy.sync(proxy.handle)(self.ep.namespace, self.ep.name,
                                           *args, **kwargs)


def call_at_once(*eps_with_args):
  """Call the given list of endpoints with arguments at once.
  Its argument list must be a list of triples (endpoint, args, kwargs).
  """
  # eps_with_args => endpoint, args, kwargs
  def get_future(arguments):
    ep, args, kwargs = arguments
    return ep(*args, **kwargs)
  return FutureCollection(*map(get_future, eps_with_args))


def endpoint(namespace=None, name=None, description=None, arg_descriptions=None,
             elevated=False, protection=NO_PROTECTION):
  """ISC endpoint definition::

      @endpoint()
      def execute(appname):
        ...

    This will expose the 'execute' function through ISC so that other nodes
    can call it. Optionally, namespace and identifier can be specified.
    passtifier can be specified.
  """
  def wrapped(f):
    register_endpoint(f, namespace, name, description, arg_descriptions,
                      elevated, protection)
    return f
  return wrapped


def register_endpoint(f, namespace=None, name=None, description=None,
                      arg_descriptions=None, elevated=False,
                      protection=NO_PROTECTION):
  try:
    arg_types = get_func_arg_types(f)
  except Exception:
    logging.exception("Failed to register ep, cannot find function \
                      argument specs.")
  if arg_descriptions:
    for arg_key in arg_descriptions.keys():
      if arg_key not in arg_types.keys():
        raise IscRegisterError("Failed to register ep, cannot find %s \
                               function in function arguments." % arg_key)

  ep_namespace = namespace or f.__module__
  ep_name = name or f.__name__
  ep_description = description or f.__doc__ or ''

  callback = Future()

  def do_register():
    # Create end point by given data.
    ep = Endpoint(ep_namespace,
                  ep_name,
                  appname=clique.runtime.app_name(),
                  elevated=elevated,
                  protection=protection,
                  description=ep_description,
                  arg_descriptions=arg_descriptions,
                  arg_types=arg_types
                  )
    key = ENDPOINT_KEY_SEPARATOR.join([ep_namespace, ep_name])
    # Add/refresh function map.
    FUNC_MAP[key] = f
    f.__dict__['endpoint'] = ep
    # Register the endpoint to the engine.
    register_ep = Endpoint('engine.isc', 'register_app_endpoint')
    register_ep(ep).then(callback.set_value)
  clique.do(do_register)
  return callback


def handle(namespace, name, *args, **kwargs):
  """Handle invoke function from app by rpc proxy.
  """
  if namespace and name:
    # TODO: local only?, check auth, arg_desc.
    key = ENDPOINT_KEY_SEPARATOR.join([namespace, name])
    func = FUNC_MAP.get(key)
    if func:
      logging.debug("Try to invoke %s function.", name)
      with rpclib.context() as c:
        node_id, appname = parse_location(c.source)
        context = Lazy()
        context.node_id = node_id
        context.appname = appname
        _set_context(context)
      return func(*args, **kwargs)
    else:
      raise IscHandleError("Failed to handle %s function, cannot find "
                           "function." % name)
  else:
    raise IscHandleError(
        "Failed to handle function, the given params is None.")


def set_access_token(token):
  """Sets the given access token.
  """
  if token:
    global TOKEN
    TOKEN = token
    logging.debug("Sets the access token by given newly token.")
  else:
    logging.warning("Failed to sets the access token, given token is None \
                    type.")


def parse_location(location):
  return tuple(location.split('.', 1))


def build_location(node_id='', appname=''):
  return '.'.join([node_id, appname])


def get_func_arg_types(func):
  """Build function arguments types as a string.
  defination ::
  {arg1 : 'required', arg2 : 'str', arg3 : 'int'}
  """
  func_args, _, _, defaults = inspect.getargspec(func)
  default_arg_index = len(func_args) - len(defaults or [])
  arg_types = dict([(func_args[i],
                     type(defaults[i - default_arg_index]).__name__)
                    for i in range(default_arg_index, len(func_args))])
  map(lambda arg: arg_types.__setitem__(arg, 'required'),
      func_args[:-len(defaults or [])] or func_args)

  return arg_types


def _set_context(context):
  __CONTEXTS__.context = context


@contextmanager
def context():
  """ISC call context.
  Available data:
    * node_id : the source node_id.
    * appname : the source appname (empty if it's directly sent by a node).
  """
  yield __CONTEXTS__.context


class IscRegisterError(Exception):
  """Exeption class for register endpoint error.
  """
  pass


class IscHandleError(RpcError):
  """Exception class for handle isc endpoint error.
  """
  def convet(self):
    return _IscHandleError(self.message)


class _IscHandleError(_RpcError):
  """Exception class for handle isc endpoint error. (internal-only)
  """
  def convet(self):
    return IscHandleError(self.msg)


class InvalidNodeNameError(RpcError):
  """Exception class for invalid node name error
  """
  def convert(self):
    return _InvalidNodeNameError(self.message)


class _InvalidNodeNameError(_RpcError):
  def convert(self):
    return InvalidNodeNameError(self.msg)


class IdentityNotFoundError(RpcError):
  """Exception class for identity not found error
  """
  def convert(self):
    return _IdentityNotFoundError(self.message)


class _IdentityNotFoundError(_RpcError):
  """Exception class for identity not found error (internal-only)
  """
  def convert(self):
    return IdentityNotFoundError(self.msg)


class EndpointNotFoundError(RpcError):
  """Exception class for endpoint not found error
  """
  def convert(self):
    return _EndpointNotFoundError(self.message)


class _EndpointNotFoundError(_RpcError):
  """Exception class for endpoint not found error (internal-only)
  """
  def convert(self):
    return EndpointNotFoundError(self.msg)


class AccessDeniedError(RpcError):
  """Exception class for access denied error
  """
  def convert(self):
    return _AccessDeniedError(self.message)


class _AccessDeniedError(_RpcError):
  """Exception class for access denied error (internal-only)
  """
  def convert(self):
    return AccessDeniedError(self.msg)


class UnauthorizedError(RpcError):
  """Exception class for unauthorized error
  """
  def convert(self):
    return _UnauthorizedError(self.message)


class _UnauthorizedError(_RpcError):
  """Exception class for unauthorized error (internal-only)
  """
  def convert(self):
    return UnauthorizedError(self.msg)
