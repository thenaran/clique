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


""" RPC server.
"""


from rpclib.tornado.tcpserver import TCPServer
from rpclib.tornado.netutil import bind_sockets, bind_unix_socket
from rpclib.proxy import _StreamClosedError
from rpclib.proxy import RpcMethodNotFoundError
from rpclib.proxy import NameSpaceNotFoundError
from rpclib.proxy import InvalidParameterError
from rpclib.proxy import TimeoutError
from rpclib.proxy import Binary
from rpclib.proxy import RpcError
from rpclib.proxy import _RpcError
from rpclib.proxy import ReadStream
from rpclib.proxy import WriteStream
from rpclib.proxy import create_stream
from rpclib.proxy import StreamToStreamCopier
from rpclib.proxy import ResponseCopier
from rpclib.proxy import SocketDataTransportor
from rpclib.proxy import WebSocketDataTransportor
from rpclib.proxy import SINGLE_REQUEST, BINARY_REQUEST
from rpclib.proxy import SHARED_REQUEST, MULTI_REQUEST
from contextlib import contextmanager
from rpclib.msgpack import Packer
from adt.concurrency import Future
from adt.concurrency import HotPotato
from adt.concurrency import Lazy
from collections import deque
import rpclib
import logging
import socket
import struct
import inspect
import proxy
import ssl
import stat
import functools


DEFAULT_TIMEOUT = 150
DEFAULT_MAX_BUFFER_SIZE = 10485760
DEFAULT_READ_CHUNK_SIZE = 32768

SIZE_VALUE_TYPE = '''L'''
SIZE_VALUE_SIZE = struct.calcsize(SIZE_VALUE_TYPE)

HEADER_TYPE_HANDLER = 1
MESSAGE_TYPE_HANDLER = 2

DEFAULT_TASK_POOL_SIZE = 5


class MultiRpcHandler(object):
  pass


class SharedRpcHandler(object):
  def __init__(self, data, transportor, ioloop, client_address,
               namespaces):
    self._data = data
    self._transportor = transportor
    self._client_address = client_address
    self._ioloop = ioloop
    self._namespaces = namespaces
    self._read_data()

  def _read_data(self):
    self._ioloop.add_callback(self._on_read)

  def _on_read(self):
    if not self._transportor.closed():
      self._transportor.read().then(self._on_data_read)

  def _on_data_read(self, data):
    try:
      if data[0] == SHARED_REQUEST:
        self._handle_data(self._namespaces, data)
      else:
        logging.debug("data is not shared request:%s",
                      str(data))
    except:
      logging.debug("Error while reading shared request data", exc_info=True)
    finally:
      self._read_data()

  def _on_timeout(self, response_id):
    logging.error("shared rpc server timed out address:%s, data:%s",
                  str(self._client_address), str(self._data))
    self._handle_result(TimeoutError("Rpc server time out"), response_id)

  def handle(self):
    self._handle_data(self._namespaces, self._data)

  def close(self):
    pass

  def _handle_data(self, namespaces, data):
    pub_key = None
    stream = self._transportor.stream
    if hasattr(stream, 'socket') and hasattr(stream.socket, 'getpeercert'):
      cert = stream.socket.getpeercert(binary_form=True)
      if cert:
        pub_key = ssl.DER_cert_to_PEM_cert(cert)

    request_type, src, dst, timeout, namespace, method, args, kwargs, response_id = data
    logging.debug("Shared rpc request serving... rpc=%s.%s, args=%s, response_id:%s",
                  namespace, method, str(args), str(response_id))

    self._hotpotato = HotPotato(self._ioloop, timeout, functools.partial(self._on_timeout,
                                                                         response_id))
    # should copy because the args and kwargs in data will be resent.
    # if args is specialized, the specialized args can't be packed
    args = proxy.specialize(args, True)
    kwargs = proxy.specialize(kwargs, True)
    if namespace in namespaces:
      obj = namespaces[namespace]
    else:
      self._handle_result(NameSpaceNotFoundError("Namespace %s not found!" %
                                                 namespace), response_id)
      return

    if hasattr(obj, method):
      func = getattr(obj, method)
    else:
      self._handle_result(RpcMethodNotFoundError(
          "Method %s not found! namespace:%s" % (method, namespace)),
          response_id)
      return

    context = Lazy()
    context.request_type = request_type
    context.response_id = response_id
    context.transportor = self._transportor
    context.client_address = self._client_address
    context.public_key = pub_key
    context.source = src
    context.destination = dst
    rpclib._set_context(context)

    try:
      result = func(*args, **kwargs)
    except Exception, e:
      logging.warn("Error on execute func:%s",
                   str(method), exc_info=True)
      result = e

    self._handle_result(result, response_id, data)

  def _handle_result(self, result, response_id, data=None):
    if isinstance(result, RoutingInfo):
      RoutingHandler(result, data, self._transportor, self._hotpotato,
                     self._ioloop, functools.partial(self._on_result, response_id),
                     lambda: None).handle()
    else:
      ResultHandler(result, self._hotpotato,
                    functools.partial(self._on_result,
                                      response_id)).handle()

  def _on_result(self, response_id, result):
    if not self._transportor.closed():
      try:
        def do_write():
          r = proxy.generalize(result)
          data = [
              SHARED_REQUEST,
              response_id,
              r
          ]
          logging.debug("Shared rpc respond:%s", str(data))
          self._transportor.write(data)
      except:
        logging.warn("Error while writing shared rpc response",
                     exc_info=True)
      self._ioloop.add_callback(do_write)
    else:
      logging.debug("stream closed the shared result dropped:%s, id:%s",
                    str(result), str(response_id))


class BinaryRpcHandler(object):
  def __init__(self, data, transportor, ioloop, client_address, namespaces):
    self._data = data
    self._transportor = transportor
    self._client_address = client_address
    self._ioloop = ioloop
    self._namespaces = namespaces
    self._input_binary = None

  def _on_timeout(self):
    logging.error("single rpc server timed out address:%s, data:%s",
                  str(self._client_address), str(self._data))
    self._handle_result(TimeoutError("Rpc server time out"))

  def handle(self):
    pub_key = None
    stream = self._transportor.stream
    if hasattr(stream, 'socket') and hasattr(stream.socket, 'getpeercert'):
      cert = stream.socket.getpeercert(binary_form=True)
      if cert:
        pub_key = ssl.DER_cert_to_PEM_cert(cert)

    request_type, src, dst, timeout, namespace, method, args, kwargs, _ = self._data
    logging.debug("Binary rpc request serving... rpc=%s.%s, args=%s",
                  namespace, method, str(args))

    self._hotpotato = HotPotato(self._ioloop, timeout, self._on_timeout)
    # should copy because the args and kwargs in data will be resent.
    # if args is specialized, the specialized args can't be packed
    args = proxy.specialize(args, True)
    kwargs = proxy.specialize(kwargs, True)

    if namespace in self._namespaces:
      obj = self._namespaces[namespace]
    else:
      self._handle_result(NameSpaceNotFoundError("Namespace %s not found!" %
                                                 namespace))
      return

    if hasattr(obj, method):
      func = getattr(obj, method)
    else:
      self._handle_result(RpcMethodNotFoundError(
          "Method %s not found! namespace:%s" % (method, namespace)))
      return

    context = Lazy()
    context.request_type = request_type
    context.transportor = self._transportor
    context.client_address = self._client_address
    context.public_key = pub_key
    context.source = src
    context.destination = dst
    rpclib._set_context(context)

    binaries = [s for s in args if isinstance(s, Binary)]
    if len(binaries) > 1:
      self._handle_result(
          InvalidParameterError("input binary can't be set more than one"))
    elif len(binaries) == 1:
      binaries[0]._set_stream(ReadStream(self._transportor.stream,
                                         self._ioloop, self._hotpotato,
                                         binaries[0].size), False)
      self._input_binary = binaries[0]

    try:
      result = func(*args, **kwargs)
    except Exception, e:
      logging.warn("Error on execute func:%s",
                   str(method), exc_info=True)
      result = e

    self._handle_result(result)

  def close(self):
    self._transportor.close()

  def _handle_result(self, result):
    if isinstance(result, RoutingInfo):
      RoutingHandler(result, self._data, self._transportor, self._hotpotato,
                     self._ioloop, self._on_result, self.close).handle()
    else:
      ResultHandler(result, self._hotpotato, self._on_result).handle()

  def _on_result(self, result):
    binary = None
    if isinstance(result, Binary):
      binary = result
    elif isinstance(result, tuple) or isinstance(result, list):
      for value in result:
        if isinstance(value, Binary):
          binary = value
          break

    if binary is not None:
      def callback(_):
        binary._set_stream(WriteStream(self._transportor.stream, self._ioloop,
                                       self._hotpotato, binary.size), True)
      self._on_respond(result, binary.size, callback)
    else:
      if self._input_binary:
        self._on_respond(result)
        self._input_binary._set_should_close(True)
      else:
        self._on_respond(result, callback=self.close)

  def _on_respond(self, result, binary_size=0, callback=None):
    if not self._transportor.closed():
      def do_write():
        try:
          r = proxy.generalize(result)
          data = [
              BINARY_REQUEST,
              binary_size,
              r
          ]
          logging.debug("Binary rpc respond:%s", str(data))
          self._transportor.write(data).then(callback)
        except:
          logging.warn("Error while response", exc_info=True)
          self.close()
      self._ioloop.add_callback(do_write)
    else:
      logging.debug("stream closed for binary request, result dropped:%s",
                    str(result))


class SingleRpcHandler(object):
  def __init__(self, data, transportor, ioloop, client_address, namespaces):
    self._data = data
    self._transportor = transportor
    self._client_address = client_address
    self._namespaces = namespaces
    self._ioloop = ioloop

  def _on_timeout(self):
    logging.error("single rpc server timed out address:%s, data:%s",
                  str(self._client_address), str(self._data))
    self._handle_result(TimeoutError("Rpc server timed out"))

  def close(self):
    self._transportor.close()

  def handle(self):
    pub_key = None
    stream = self._transportor.stream
    if hasattr(stream, 'socket') and hasattr(stream.socket, 'getpeercert'):
      cert = stream.socket.getpeercert(binary_form=True)
      if cert:
        pub_key = ssl.DER_cert_to_PEM_cert(cert)

    request_type, src, dst, timeout, namespace, method, args, kwargs = self._data
    logging.debug("Single rpc request serving... rpc=%s.%s, args=%s",
                  namespace, method, str(args))

    self._hotpotato = HotPotato(self._ioloop, timeout, self._on_timeout)
    # should copy because the args and kwargs in data will be resent.
    # if args is specialized, the specialized args can't be packed
    args = proxy.specialize(args, True)
    kwargs = proxy.specialize(kwargs, True)

    if namespace in self._namespaces:
      obj = self._namespaces[namespace]
    else:
      self._handle_result(NameSpaceNotFoundError("Namespace %s not found!" %
                                                 namespace))
      return

    if hasattr(obj, method):
      func = getattr(obj, method)
    else:
      self._handle_result(RpcMethodNotFoundError(
          "Method %s not found! namespace:%s" % (method, namespace)))
      return

    context = Lazy()
    context.request_type = request_type
    context.transportor = self._transportor
    context.client_address = self._client_address
    context.public_key = pub_key
    context.source = src
    context.destination = dst
    rpclib._set_context(context)

    try:
      result = func(*args, **kwargs)
    except Exception, e:
      logging.warn("Error on execute func:%s",
                   str(method), exc_info=True)
      result = e

    logging.debug("single handle result:%s", str(result))
    self._handle_result(result)

  def _handle_result(self, result):
    if isinstance(result, RoutingInfo):
      RoutingHandler(result, self._data, self._transportor, self._hotpotato,
                     self._ioloop, self._on_result, self.close).handle()
    else:
      ResultHandler(result, self._hotpotato, self._on_result).handle()

  def _on_result(self, result):
    if not self._transportor.closed():
      def do_write():
        try:
          r = proxy.generalize(result)
          data = [
              SINGLE_REQUEST,
              r
          ]
          logging.debug("single rpc respond:%s", str(data))
          self._transportor.write(data).then(self.close)
        except:
          logging.warn("Error while writing single rpc response",
                       exc_info=True)
      self._ioloop.add_callback(do_write)
    else:
      logging.debug("stream closed the result is dropped:%s",
                    str(result))


class RoutingHandler(object):
  def __init__(self, info, data, transportor, hotpotato, ioloop,
               result_handler, finish_handler):
    self._info = info
    self._transportor = transportor
    self._data = data
    self._hotpotato = hotpotato
    self._ioloop = ioloop
    self._result_handler = result_handler
    self._finish_handler = finish_handler
    self._packer = Packer(encoding='utf-8')

  def handle(self):
    self._hotpotato.start()
    logging.debug("handle routing address:%s, source:%s",
                  str(self._info.address), str(self._info.source))
    target = create_stream(self._info.address, self._ioloop,
                           ssl_options=self._info.ssl_options)

    logging.debug("start connect")
    try:
      target.connect(self._info.address, functools.partial(self._on_connected, target))
    except:
      logging.debug("Error while connecting address:%s for routing",
                    str(self._info.address), exc_info=True)

  def _on_connected(self, stream):
    self._hotpotato.stop()
    logging.debug("connected routing")
    if stream and stream.closed():
      self._handle_result(_StreamClosedError("stream of addr:%s is closed" %
                                             str(self._info.address)))
    else:
      try:
        self._data[1] = self._info.source
        logging.debug("routing data:%s", str(self._data))
        raw = self._packer.pack(self._data)
        size = struct.pack(SIZE_VALUE_TYPE, len(raw))
        stream.write(size)
        stream.write(raw)

        if self._data[0] == BINARY_REQUEST:
          StreamToStreamCopier(self._transportor.stream, stream,
                               self._data[8], self._hotpotato,
                               lambda c: None).start()

        def finish():
          self._finish_handler()
          stream.close()

        ResponseCopier(stream, self._transportor.stream, self._ioloop,
                       self._hotpotato, finish).start()

      except:
        logging.debug("Error while routing data:%s",
                      str(self._data), exc_info=True)
        self._handle_result(_RpcError("Error while routing data:%s" %
                                      str(self._data)))

  def _handle_result(self, result):
    self._result_handler(result)


class ResultHandler(object):
  def __init__(self, result, hotpotato, result_handler):
    self._result = result
    self._hotpotato = hotpotato
    self._result_handler = result_handler

  def handle(self):
    self._handle_response()

  def _convert_result(self, result):
    if isinstance(result, RpcError):
      result = result.convert()
    elif isinstance(result, BaseException):
      result = proxy._RpcError(str(result))

    return result

  def _handle_response(self):
    try:
      if isinstance(self._result, Future):
        def value_callback(value):
          value = self._convert_result(value)
          self._hotpotato.stop()
          self._handle_result(value)
        self._hotpotato.start()
        self._result.get_value(value_callback)
      else:
        value = self._convert_result(self._result)
        self._handle_result(value)
    except Exception, e:
      logging.exception("Error while responding thre result:%s",
                        str(self._result))
      self._handle_result(_RpcError(str(e)))

  def _handle_result(self, result):
    self._result_handler(result)


class Resolver(object):

  __HANDLERS__ = {
      SINGLE_REQUEST: SingleRpcHandler,
      BINARY_REQUEST: BinaryRpcHandler,
      SHARED_REQUEST: SharedRpcHandler,
      MULTI_REQUEST: MultiRpcHandler
  }

  def __init__(self, transportor, address, ioloop,
               max_buffer_size=DEFAULT_MAX_BUFFER_SIZE,
               read_chunk_size=DEFAULT_READ_CHUNK_SIZE):
    self._transportor = transportor
    self._address = address
    self._ioloop = ioloop
    self._packer = Packer(encoding='utf-8')

  @property
  def context(self):
    return self._context

  def handle(self, context):
    self._context = context
    self._ioloop.add_callback(self._do_read)

  def _on_timeout(self):
    logging.warn("time out on reading first data from address:%s",
                 str(self._address))

  def _do_read(self):
    self._hotpotato = HotPotato(self._ioloop, DEFAULT_TIMEOUT,
                                self._on_timeout)
    logging.debug("start read in resolver")
    self._transportor.read().then(self._on_data_read)

  def _on_data_read(self, data):
    try:
      self._data = data
      request_type = data[0]

      handler = self.__HANDLERS__.get(request_type)
      if handler:
        logging.debug("start handle!")
        handler(self._data, self._transportor, self._ioloop,
                self._address, self._context).handle()
      else:
        logging.debug("invalid request type:%s",
                      str(request_type))
    except:
      logging.warn("Error while reading data:%s",
                   str(data), exc_info=True)


class RoutingInfo(object):
  """The RoutingInfo has source and target address for routing
  """
  def __init__(self, source, address, ssl_options=None):
    self._source = source
    self._address = address
    self._ssl_options = ssl_options

  @property
  def source(self):
    return self._source

  @property
  def address(self):
    return self._address

  @property
  def ssl_options(self):
    return self._ssl_options


class FuncObj(object):
  """Function object.
  """
  def __init__(self, instance=None):
    self._funcs = {}

    if instance:
      map(lambda t: self.__setitem__(*t),
          [m for m in inspect.getmembers(instance, inspect.ismethod)])

  def __setitem__(self, name, value):
    if not name.startswith("_"):
      if hasattr(value, '__call__'):
        if name in self._funcs:
          logging.warn("Function gets overriden. name=%s", name)
        self._funcs[name] = value
      else:
        logging.debug("name:%s of value:%s is not callable",
                      str(name), str(value))
    else:
      logging.debug("name:%s can't be started with '_'", str(name))

  def __getattr__(self, name):
    if name.startswith("_"):
      return object.__getattr__(self, name)
    func = self._funcs[name]
    return func


class RpcServer(object):
  """RPC server abstraction for receiving and handling RPC messages.
  `ident` must be provided as it represents the identity of the server instance
  (but it does not need to be unique).

  `RpcServer` provides a routing feature, which basically allows multiple
  `RpcServer`s to be clustered together. One `RpcServer` can pass its request
  to the other only if the `dest_id` is exist.
  """

  def __init__(self, ioloop=None, encoding='utf-8',
               timeout=DEFAULT_TIMEOUT,
               max_buffer_size=DEFAULT_MAX_BUFFER_SIZE,
               read_chunk_size=DEFAULT_READ_CHUNK_SIZE,
               ssl_options=None, **kwargs):
    self._ioloop = ioloop or rpclib.ioloop()
    self._namespaces = {}
    self._encoding = encoding
    self._timeout = timeout
    self._read_chunk_size = read_chunk_size
    self._max_buffer_size = max_buffer_size
    self.initialize(ssl_options=ssl_options, **kwargs)

  def __del__(self):
    try:
      self.stop()
    except:
      logging.exception("Failed to dispose the RpcServer.", exc_info=True)

  def stop(self):
    """Stop the RPC server.
    """
    # Dispose the underlying transport.
    self.dispose()

  def context(self):
    return self._namespaces

  def clear(self):
    self._namespaces.clear()

  def _get_or_create_obj(self, namespace, default_obj=None):
    obj = self._namespaces.get(namespace)
    if obj is None:
      obj = FuncObj(default_obj)
      self._namespaces[namespace] = obj
    return obj

  def register_function(self, func, namespace='', name=None):
    """Expose the given function. Registering under the same namespace
    will result in overriding without warning.
    """
    obj = self._get_or_create_obj(namespace)
    name = name or func.__name__
    obj[name] = func

  def register_instance(self, instance, namespace=''):
    """Expose the given instance's methods. Registering under the same namespace
    will result in overriding without warning.
    """
    self._get_or_create_obj(namespace, default_obj=instance)

  def handle_uncaught_exception(self, exc):
    """Override to handle any uncaught exception.
    """
    pass

  def do_handle(self, transportor, address):
    Resolver(transportor, address, self._ioloop).handle(self.context())


class WebSocket(object):

  def __init__(self, write_handler=None):
    logging.debug("web socket initialized")
    self._write_handler = write_handler
    self.data = deque()
    self.ft = deque()
    self._close_callback = None
    self._closed = False

  def set_write_handler(self, write_handler):
    self._write_handler = write_handler

  def handle_message(self, message):
    if message:
      if len(self.ft) > 0:
        try:
          ft = self.ft.popleft()
          ft.set_value(message)
        except:
          logging.warn("fail to handle websocket message:%s",
                       str(message), exc_info=True)
      else:
        self.data.append(message)
    else:
      logging.debug("message is None or Empty:%s", str(message))

  def _handle_close(self):
    if self._close_callback:
      cb = self._close_callback
      self._close_callback = None
      cb()
    self._closed = True

  def read(self):
    if len(self.data) > 0:
      try:
        message = self.data.popleft()
        return Future(value=message)
      except:
        logging.warn("fail to handle websocket message:%s",
                     str(message), exc_info=True)
    else:
      logging.debug("no message return ft")
      ft = Future()
      self.ft.append(ft)
      return ft

  def write(self, data):
    try:
      if self._write_handler:
        logging.debug("write data:%s", str(data))
        self._write_handler(data)
    except:
      logging.warn("fail to write websocket data:%s",
                   str(data), exc_info=True)

  def set_close_callback(self, callback):
    self._close_callback = callback

  def closed(self):
    return self._closed

  def close(self):
    self._handle_close()


class WebSocketTransport(object):
  """Non-blocking, single-threaded websocket transport.
  """
  def initialize(self, **kwargs):
    pass

  def add_stream(self, stream):
    logging.debug("stream added")
    self.handle_stream(stream, '')

  def handle_stream(self, stream, address):
    logging.debug("handle websocket stream")
    self.do_handle(WebSocketDataTransportor(stream), address)

  def dispose(self):
    pass

  def serve(self):
    pass


class TcpSocketTransport(object):
  """Non-blocking, single-threaded TPC transport.

     It support SSL with OpenSSL, To enable SSL ssl options must be set

     >>> ssl_options={
           "certfile": os.path.join(data_dir, "mydomain.crt"),
           "keyfile": os.path.join(data_dir, "mydomain.key"),
           })
     >>> TcpServer(RpcHandler, ssl_options=ssl_option)

     Before using TcpServer, should call initialize method
  """
  def initialize(self, ssl_options=None, port=0, **kwargs):
    self._server = TCPServer(io_loop=self._ioloop, ssl_options=ssl_options)
    self._server.handle_stream = self.handle_stream
    self._port = port

  def handle_stream(self, stream, address):
    stream.max_buffer_size = self._max_buffer_size
    stream.read_chunk_size = self._read_chunk_size
    self.do_handle(SocketDataTransportor(stream), address)

  def dispose(self):
    self._server.stop()

  def serve(self):
    """Start the server on any open port.
    """
    sockets = bind_sockets(self._port, address='', family=socket.AF_INET)
    self._server.add_sockets(sockets)
    if len(sockets) > 0:
      self._port = sockets[0].getsockname()[1]
      return self._port
    return -1

  @property
  def port(self):
    return self._port


class UnixSocketTransport(object):
  """Unix socket server is Non-blocking, single, thread TPC server.
  """
  def initialize(self, path=None, chmod=None, **kwargs):
    self._server = TCPServer(io_loop=self._ioloop)
    self._server.handle_stream = self.handle_stream
    self._path = path
    self._chmod = chmod or \
        (stat.S_IRUSR | stat.S_IRGRP | stat.S_IWUSR | stat.S_IWGRP)

  def dispose(self):
    self._server.stop()

  @contextmanager
  def handle_exception(self):
    try:
      yield
    except:
      logging.exception("Error while handle stream on unix socket",
                        exc_info=True)

  def handle_stream(self, stream, address):
    logging.debug("unix handle stream")
    stream.max_buffer_size = self._max_buffer_size
    stream.read_chunk_size = self._read_chunk_size
    self.do_handle(SocketDataTransportor(stream), address)

  def serve(self):
    """Start the server on binded local path.
    """
    sock = bind_unix_socket(self._path, mode=self._chmod)
    if sock:
      self._server.add_socket(sock)
      return self._path
    return None


class RpcServerOverTcp(RpcServer, TcpSocketTransport):
  pass


class RpcServerOverUnixSocket(RpcServer, UnixSocketTransport):
  pass


class RpcServerOverWebSocket(RpcServer, WebSocketTransport):
  pass
