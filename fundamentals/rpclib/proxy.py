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


""" RPC proxy.


RPC protocol is defined in the following way.

  * all primitive types are serialized using msgpack
  * custom object types must inherit from `Base` in order for them to be properly deserializable at the endpoint. Otherwise, they are converted to plain dictionaries.
  * binary data can be transmitted in a streaming manner if it's large enough not to fit in the memory buffer.

Each message has the following structure::

    request -> request-message

    request-message -> size header type namespace func-name list-of-args dict-of-kwargs large-binary-data

    header -> socket_id response_id source dest timeout binary_size

    dest -> string-identifier

    response -> response-message

    response-message -> size response_id serialized-data large-binary-data

    large-binary-data -> binary


note that large binary data (larger than the buffer size) comes after the actual message.


The rest of the CFGs::

    namespace, func-name, key -> string

    list-of-args -> list( arg, args )

    args -> arg[, args] | e

    arg -> primitive | `Base`

    dict-of-kwargs -> dict( { 'key': arg, kwargs } )

    kwargs -> 'key': arg[, kwargs] | e

"""

from cStringIO import StringIO
from rpclib.msgpack import Packer, Unpacker
from rpclib.tornado.iostream import IOStream
from rpclib.tornado.iostream import SSLIOStream
from rpclib.tornado.platform import auto
from adt.concurrency import Future
from adt.concurrency import HotPotato
from adt.funcs import try_execute
import tornado.escape
import adt.funcs
import ast
import rpclib
import copy
import functools
import struct
import socket
import logging
import uuid


SINGLE_REQUEST = 1
BINARY_REQUEST = 2
SHARED_REQUEST = 3
MULTI_REQUEST = 4

SIZE_VALUE_TYPE = '''L'''
SIZE_VALUE_SIZE = struct.calcsize(SIZE_VALUE_TYPE)

NO_RESULT_EXCEPTION = Exception("No result received.")

DEFAULT_TIMEOUT = 30
DEFAULT_MAX_BUFFER_SIZE = 1048576000
DEFAULT_READ_CHUNK_SIZE = 32768
REQUEST_THRESHOLD = 3145728  # 3MB


class Base(object):
  """Abstract base type for remote communication supporting two operations:
  `generalize` and `specialize`.

    `generalize` : converting all custom base types into primitive types like dictionaries recursively.

    `specialize` : converting back to original types recursively.
  """
  def copy(self):
    """Shallow-copy the base object.
    """
    return copy.copy(self)

  def __repr__(self):
    s = StringIO()
    s.write(self.__class__.__name__ + ': {')
    for k, v in self.__dict__.iteritems():
      s.write(k + ': ' + str(v))
      s.write(', ')
    if len(self.__dict__) > 0:
      s.seek(-2, 1)
    s.write('}')
    ret = s.getvalue()
    s.close()
    return ret


def generalize(obj):
  """Convert the given object into primitive types recursively.
  """
  # - Replace all Base objects to dictionaries.
  # - The dictionaries should contain ___module___ and ___class___ attributes.
  # - This function must not call itself recursively. Use the collection deque.
  # - It must raise NotGeneralizableError exception if plain objects are contained.
  if obj is None:
    return None

  ret = adt.funcs.replace(lambda k, v: isinstance(v, Base), obj,
                          lambda k, v: _do_generalize(v), True)
  return ret


def _do_generalize(obj):
  """Helper to generalize the given object, filtering all private properties.
  """
  for key in obj.__dict__.keys():
    if key.startswith('_') and not key.startswith('__'):
      del obj.__dict__[key]
  obj.___classname___ = obj.__class__.__name__
  obj.___module___ = obj.__module__
  return obj.__dict__


def specialize(obj, copy=False):
  """Convert back the given object into the orginal types if not primitive.
  """
  # - Replace all dictionaires with __module__ and __class atributes to the original objects.
  # - Use __import__ when importing the original type.
  # - Use __new__ to create the object and replace the __dict__ with the dictionary.
  # - If failed, just create a Base object and replace its __dict__ with the dictionary.
  # - This function must not call itself recursively. Use the collection deque.
  if obj is None:
    return None

  ret = adt.funcs.replace(lambda k, v: isinstance(v, dict) and
                          '___classname___' in v and '___module___' in v,
                          obj,
                          lambda k, v: _do_specialize(v), copy)
  return ret


def _do_specialize(obj_data):
  """Helper to adapt the given obj must be adaptable.
  """
  modulename = obj_data['___module___']
  classname = obj_data['___classname___']
  try:
    m = __import__(modulename, fromlist=[classname])
    cls = getattr(m, classname)
    obj = object.__new__(cls)
    obj.__dict__ = obj_data
  except:
    logging.info("Failed to import the original type. \
                 cls=%s, module=%s",
                 obj_data['___classname___'],
                 obj_data['___module___'],
                 exc_info=True)
    obj = Base()
    obj.__dict__ = obj_data
    logging.info("Generic object is created. data:%s", str(obj_data))
  return obj


class DataTransportor(object):
  def __init__(self, stream):
    self._rft = None
    self._is_reading = False
    self._wft = None
    self._data = None
    self._stream = stream
    self._routing_map = {}
    self._stream.set_close_callback(self._on_closed)

  def _on_closed(self):
    self.dispose()

  def dispose(self):
    for target in self._routing_map:
      try:
        target.close()
      except:
        logging.debug("fail to close target routing stream",
                      exc_info=True)

  def reading(self):
    return self._is_reading

  def read(self):
    if self.reading():
      raise Exception("transportor is reading data")
    self._rft = Future()
    self._is_reading = True
    self.do_read()
    return self._rft

  def do_read(self):
    pass

  def write(self, data):
    self._wft = Future()
    self.do_write(data)
    return self._wft

  def do_write(self, data):
    pass

  def add_routing_connection(self, addr, target):
    self._routing_map[addr] = target

  def get_routing_connection(self, addr):
    self._routing_map.get(addr)

  def remove_routing_connection(self, addr):
    try:
      del self._routing_map[addr]
    except:
      logging.warn("fail to remove routing connection of addr:%s",
                   str(addr), exc_info=True)

  def closed(self):
    return self._stream.closed()

  def close(self):
    self._stream.close()

  @property
  def data(self):
    # The data is not thread safe
    return self._data

  @property
  def stream(self):
    return self._stream


class SocketDataTransportor(DataTransportor):
  """Extract socket data
  """
  def __init__(self, stream, encoding='utf-8'):
    super(SocketDataTransportor, self).__init__(stream)
    self._packer = Packer(encoding=encoding)

  def do_read(self):
    self._unpacker = Unpacker(use_list=True)
    self._stream.read_bytes(SIZE_VALUE_SIZE, self._on_size_received)

  def _on_size_received(self, data):
    size = struct.unpack(SIZE_VALUE_TYPE, data)[0]
    self._stream.read_bytes(size, self._on_request_received)

  def _on_request_received(self, data):
    try:
      self._unpacker.feed(data)
      for unpacked in self._unpacker:
        self._request_ready = True
        self._data = unpacked
        self._is_reading = False
        if self._rft:
          self._rft.set_value(self._data)
        else:
          logging.debug("response future not exist for data:%s",
                        str(self._data))
        return
      logging.error("No valid request has been received")
    except Exception, e:
      logging.warn("Error while unpack data:%s", str(data),
                   exc_info=True)
      if self._rft:
        self._rft.set_value(e)

  def do_write(self, data):
    r = self._packer.pack(data)
    l = struct.pack(SIZE_VALUE_TYPE, len(r))
    self._stream.write(l)
    self._stream.write(r, lambda: self._wft.set_value(None))


class WebSocketDataTransportor(DataTransportor):
  """Extract websocket data
  """
  def __init__(self, stream):
    super(WebSocketDataTransportor, self).__init__(stream)

  def do_read(self):
    self._stream.read().then(self._on_request_received)

  def _on_request_received(self, data):
    result = []
    try:
      data = ast.literal_eval(data)
      result.append(data.get('request_type'))
      result.append(data.get('src'))
      result.append(data.get('dst'))
      result.append(data.get('timeout'))
      result.append(data.get('namespace'))
      result.append(data.get('name'))
      result.append(data.get('args'))
      result.append(data.get('kwargs'))
      result.append(data.get('response_id'))
      self._data = result
    except:
      logging.debug("fail to generate data from message:%s in websocket data",
                    str(self._message), exc_info=True)
    self._is_reading = False
    if self._rft:
      self._rft.set_value(self._data)

  def do_write(self, data):
    try:
      request_type = data[0]
      if request_type == SHARED_REQUEST:
        result = dict(
            request_type=data[0],
            response_id=data[1],
            result=data[2]
        )
        message = tornado.escape.json_encode(result)
        self._stream.write(message)
        if self._wft:
          self._wft.set_value(None)
      else:
        logging.debug("request type:%s not support", str(request_type))
    except:
      logging.debug("Error while write data:%s",
                    str(data), exc_info=True)


class Stream(object):
  def __init__(self, stream, ioloop, hotpotato,
               size, pos=0):
    self._stream = stream
    self._stream.set_close_callback(self.on_closed)
    self._ioloop = ioloop
    self._size = size
    self._hotpotato = hotpotato
    self._pos = pos
    self._error = None
    self._close_callback = None
    self._finish_callback = None
    self._hotpotato.start()

  @property
  def size(self):
    """Total byte size.
    """
    return self._size

  @property
  def pos(self):
    return self._pos

  @property
  def error(self):
    return self._error

  def writing(self):
    return self._stream.writing()

  def close(self):
    def callback():
      if not self.writing() and not self._stream.closed():
        self._stream.close()
    self._ioloop.add_callback(callback)

  def closed(self):
    return self._stream.closed()

  def on_closed(self):
    self._hotpotato.stop()
    if self._close_callback:
      cb = self._close_callback
      self._close_callback = None
      cb()

  def set_close_callback(self, close_callback):
    self._close_callback = close_callback

  def done(self):
    """Check if the stream is done.
    """
    pass


class ReadStream(Stream):
  def __init__(self, stream, ioloop, hotpotato, size, pos=0):
    super(ReadStream, self).__init__(stream, ioloop, hotpotato,
                                     size, pos)
    self._streaming_callback = None
    self._end_callback = None
    self._received_size = 0
    self._hotpotato.start()

  def read(self, streaming_callback, end_callback, num_bytes=None):
    """Call data_callback when we read the given number of bytes

    if a 'streaming_callback' is given, it will be called with chunks of
    data as they become available, and the argument to the final
    'data_callback' will be empty
    """
    num_bytes = num_bytes or self.size
    self._end_callback = end_callback
    self._streaming_callback = streaming_callback
    self._hotpotato.start()
    self._stream.read_bytes(num_bytes, self._on_finish,
                            self._on_bytes_received)

  def _on_bytes_received(self, data):
    self._hotpotato.start()
    if self._received_size < self.size:
      self._streaming_callback(data)
      self._received_size += len(data)
    #don't close stream, stream should be closed in finish callback by user

  def _on_finish(self, data):
    self._hotpotato.start()
    cb = self._streaming_callback
    self._streaming_callback = None
    if data and len(data) > 0:
      self._received_size += len(data)
      cb(data)
    if self._end_callback:
      cb = self._end_callback
      self._end_callback = None
      cb()

  def on_closed(self):
    super(ReadStream, self).on_closed()
    if self._end_callback:
      cb = self._end_callback
      self._end_callback = None
      cb()

  def done(self):
    return self._received_size >= self.size


class WriteStream(Stream):
  def __init__(self, stream, ioloop, hotpotato, size, pos=0):
    super(WriteStream, self).__init__(stream, ioloop, hotpotato,
                                      size, pos)
    self._write_callback = None
    self._close_requested = False
    self._written_size = 0
    self._hotpotato.start()

  def _on_written(self):
    if self._write_callback:
      cb = self._write_callback
      self._write_callback = None
      cb()

    if self._close_requested:
      self.close()
    else:
      self._hotpotato.start()

  def write(self, data, flush_callback=None):
    def write_data():
      self._hotpotato.start()
      try:
        if self._written_size + len(data) > self._size:
          raise Exception("Data exceeded")
        self._written_size += len(data)
        self._stream.write(data, self._on_written)
      except Exception, e:
        logging.exception("Error while writing to Stream")
        self._error = e
        self._on_written()
    self._write_callback = flush_callback
    self._ioloop.add_callback(write_data)

  def close(self):
    self._close_requested = True
    super(WriteStream, self).close()

  def done(self):
    return self._written_size >= self.size


class Binary(Base):
  """Binary data representation.
  """
  def __init__(self, size, ready_callback):
    self.size = size
    self._ready_callback = ready_callback
    self._stream = None
    self._should_close = False
    self._finished = False

  def write(self, data, flush_callback=None):
    self._stream.write(data, flush_callback)

  def read(self, streaming_callback, end_callback, num_bytes=None):
    self._stream.read(streaming_callback, end_callback, num_bytes)

  def finish(self):
    self._finished = True
    if self._should_close:
      self._stream.close()

  def closed(self):
    return self._stream.closed()

  @property
  def error(self):
    return self._stream.error

  def _set_stream(self, stream, should_close):
    self._set_stream = None
    self._stream = stream
    self._should_close = should_close
    self._finished = stream.closed()
    if hasattr(self, "_ready_callback"):
      self._ready_callback(self)

  def _set_should_close(self, should_close):
    """Only called internally to set it to auto-close when finished.
    """
    self._should_close = should_close
    if self._finished:
      self.finish()


class ResponseCopier(object):
  """Copy the source stream into the target stream.
  """
  def __init__(self, source, target, ioloop, hotpotato, finish_callback=None):
    self._source = source
    self._target = target
    self._ioloop = ioloop
    self._hotpotato = hotpotato
    self._finish_callback = finish_callback
    self._unpacker = Unpacker(use_list=True)

  def start(self):
    self._hotpotato.start()
    self._source.read_bytes(SIZE_VALUE_SIZE,
                            self._on_data_size_received)

  def _on_data_size_received(self, size_raw):
    # write size
    size = struct.unpack(SIZE_VALUE_TYPE, size_raw)[0]

    def finish_callback(data):
      self._unpacker.feed(data)
      result = None
      for unpacked in self._unpacker:
        result = unpacked
      self._target.write(size_raw)
      self._target.write(data)
      self._hotpotato.start()
      binary_size = 0
      if result[0] == BINARY_REQUEST:
        _, binary_size, _ = result
      self._on_binary_size_received(binary_size)
    self._source.read_bytes(size, finish_callback)

  def _on_binary_size_received(self, size):
    self._hotpotato.start()
    if size > 0:
      def finish_callback():
        self._finish_callback()
      StreamToStreamCopier(self._source, self._target, size,
                           self._hotpotato, finish_callback).start()
    else:
      self._hotpotato.stop()
      self._finish_callback()


class StreamToStreamCopier(object):
  """Copy stream to stream
  the 'source' and 'target' is IOStream in tornado
  """
  def __init__(self, source, target, size, hotpotato, finish_callback=None,
               auto_close=False):
    self._source = source
    self._target = target
    self._size = size
    self._finish_callback = finish_callback
    self._auto_close = auto_close
    self._hotpotato = hotpotato
    self._written_size = 0

  def start(self):
    self._hotpotato.start()

    def streaming_callback(data):
      self._hotpotato.start()
      self._write_data(data)
      self._written_size = self._written_size + len(data)
      self._check_finished()

    def finish_callback(data):
      # the finish callback is called several times during reading bytes
      self._hotpotato.stop()
      if data and len(data) > 0:
        self._write_data(data)
        self._written_size = self._written_size + len(data)
      self._check_finished()

    try:
      self._source.read_bytes(self._size, streaming_callback, finish_callback)
    except Exception, e:
      logging.exception("Error while reading from source in stream to stream")
      self._handle_error(e)

  def _handle_error(self, e):
    self._error = e
    self._on_finished()
    self._source = None
    self._target = None

  def _check_finished(self):
    if self._written_size >= self._size:
      self._on_finished()

  def _write_data(self, data):
    try:
      if self._target:
        self._target.write(data)
    except Exception, e:
      logging.exception("Error while copy stream to stream")
      self._handle_error(e)

  def _on_finished(self):
    self._hotpotato.stop()
    if self._auto_close:
      try_execute(self._source.close)
      try_execute(self._target.close)

    if self._finish_callback:
      cb = self._finish_callback
      self._finish_callback = None
      try_execute(cb, self)


class BinaryToFileCopier(object):
  """Copy binary data to file
  the 'source' is rpclib.proxy.binary class
  the 'target' is file object
  """
  def __init__(self, source, target, size, finish_callback=None,
               auto_close=False):
    self._source = source
    self._target = target
    self._size = size
    self._finish_callback = finish_callback
    self._auto_close = auto_close
    self._bytes_written = 0
    # TODO: check if the size is smaller or equal to the source binary size.
    self._error = None

  @property
  def error(self):
    return self._error

  def start(self):
    try:
      self._source.read(self._write_data, lambda: self._on_finished(),
                        self._source.size)
    except Exception, e:
      logging.exception("Error while reading from source in binary to file copier")
      self._handle_error(e)

  def _handle_error(self, e):
    self._error = e
    self._on_finished()
    self._source = None
    self._target = None

  def _write_data(self, data):
    try:
      if self._target:
        self._target.write(data)
        self._bytes_written += len(data)
    except Exception, e:
      logging.exception("Error while copy stream to stream")
      self._handle_error(e)

  def _on_finished(self):
    if self._auto_close:
      try_execute(self._source.finish)
      try_execute(self._target.close)

    if self._finish_callback:
      cb = self._finish_callback
      self._finish_callback = None
      try_execute(cb, self)

    self._source = None
    self._target = None


class FileToBinaryCopier(object):
  """Copy file data to binary
  the 'soure' is file object
  the 'target' is rpclib.proxy.binary class
  """
  def __init__(self, source, target, size, finish_callback=None,
               auto_close=False):
    self._source = source
    self._target = target
    self._size = size
    self._finish_callback = finish_callback
    self._auto_close = auto_close
    # TODO: check if the size is smaller or equal to the source file size.
    # TODO: check if the given size and the target binary size are equal.
    self._bytes_read = 0
    self._error = None

  @property
  def error(self):
    return self._error

  def start(self):
    def flush():
      try:
        if self._source and self._target:
          if self._target.error:
            self._error = self._target.error
            self._on_finished()
          else:
            if self._bytes_read + DEFAULT_READ_CHUNK_SIZE > self._size:
              read_size = self._size - self._bytes_read
            else:
              read_size = DEFAULT_READ_CHUNK_SIZE
            if read_size <= 0:
              # Finish if we have read enough.
              self._on_finished()
            else:
              d = self._source.read(read_size)
              if len(d) > 0:
                self._bytes_read += len(d)
                self._target.write(d, flush)
              else:
                self._on_finished()
      except Exception, e:
        logging.exception("Error while copy file to binary")
        self._handle_error(e)
    flush()

  def _handle_error(self, e):
    self._error = e
    self._on_finished()
    self._source = None
    self._target = None

  def _on_finished(self):
    if self._auto_close:
      try_execute(self._source.close)
      try_execute(self._target.finish)

    if self._finish_callback:
      cb = self._finish_callback
      self._finish_callback = None
      try_execute(cb, self)

    self._source = None
    self._target = None


class SingleRpcProxy(object):
  def __init__(self, address, namespace='', src='', dst='',
               ioloop=None, encoding='utf-8',
               max_buffer_size=DEFAULT_MAX_BUFFER_SIZE,
               read_chunk_size=DEFAULT_READ_CHUNK_SIZE,
               ssl_options=None, timeout=DEFAULT_TIMEOUT):
    self._address = isinstance(address, str) and address or tuple(address)
    self._src = src
    self._dst = dst
    self._namespace = namespace
    self._ioloop = ioloop or rpclib.ioloop()
    self._encoding = encoding
    self._max_buffer_size = max_buffer_size
    self._read_chunk_size = read_chunk_size
    self._ssl_options = ssl_options
    self._timeout = timeout or DEFAULT_TIMEOUT
    self._name = None
    self._request_sent = False
    self._connected = False
    self._result = NO_RESULT_EXCEPTION
    self._hotpotato = None

  def __getattr__(self, name):
    if name.startswith('_'):
      return object.__getattribute__(self, name)
    self._name = name
    return self

  def __repr__(self):
    return 'SingleRpcProxy(' + ', '.join([str(self._address), self._namespace,
                                          str(self._name), self._dst]) + ')'

  def __call__(self, *args, **kwargs):
    if not self._name:
      return Future(value=Exception("method name is not defined!"))

    streams = [s for s in args if isinstance(s, Binary)]
    if len(streams) > 0:
      return Future(value=InvalidParameterError('Binary is not supported'))

    self._args = [generalize(x) for x in args]
    self._kwargs = dict(
        [(k, generalize(v)) for k, v in kwargs.items() if not k.startswith("_")])

    try:
      self._stream = create_stream(self._address, self._ioloop,
                                   self._max_buffer_size,
                                   self._read_chunk_size,
                                   self._ssl_options)
      if self._stream:
        self._stream.set_close_callback(self._on_close)
      else:
        return Future(value=IOError("Fail to create stream address:%s",
                                    str(self._address)))
    except Exception, e:
      logging.error("Error while creating stream")
      return Future(value=e)

    request_type = SINGLE_REQUEST

    self._request = [
        request_type,
        self._src,
        self._dst,
        self._timeout,
        self._namespace,
        self._name,
        self._args,
        self._kwargs
    ]
    logging.debug("request single rpc namespace:%s, name:%s, address:%s",
                  str(self._namespace), str(self._name), str(self._address))
    self._future = Future()
    self._ioloop.add_callback(self._send_request)
    return self._future

  def _on_timeout(self):
    logging.error("RPC proxy timed out. namespace:%s, name:%s",
                  str(self._namespace), str(self._name))
    self._result = TimeoutError("Rpc timeout")
    self._fire_callback()
    self._stream.close()

  def _on_close(self):
    pass

  def _fire_callback(self):
    if self._future:
      self._future.set_value(self._result)

  def _send_request(self):
    try:
      self._hotpotato = HotPotato(self._ioloop, self._timeout,
                                  self._on_timeout)
      self._hotpotato.start()
      self._stream.connect(self._address, self._on_connected)
    except Exception, e:
      logging.exception("Failed to connect")
      self._result = e
      self._fire_callback()

  def _on_connected(self):
    self._connected = True
    try:
      self._transportor = SocketDataTransportor(self._stream)
      self._transportor.write(self._request).then(self._on_request_sent)
    except Exception, e:
      logging.exception("Failed to write data, name:%s address:%s",
                        str(self._name), str(self._address))
      self._result = e
      self._fire_callback()

  def _on_request_sent(self, _):
    logging.debug("request sent!!")
    self._hotpotato.stop()
    self._request_sent = True
    self._hotpotato.start()
    logging.debug("start reading in sent!!")
    self._transportor.read().then(self._handle_callback)

  def _handle_callback(self, result):
    logging.debug("handle callback result:%s", str(result))
    self._hotpotato.stop()
    if not self._connected:
      self._result = IOError("Failed to connect the stream")
      self._fire_callback()
    elif self._future:
      try:
        _, result = result
        if not isinstance(result, Exception):
          result = specialize(result)
          if isinstance(result, _RpcError):
            result = result.convert()
          elif isinstance(result, Binary):
            result = Exception("Binary result not supported")
          elif isinstance(result, list):
            for value in result:
              if isinstance(value, Binary):
                result = Exception("Binary result not supported")
                break
      finally:
        self._result = result
        self._fire_callback()
        self._stream.close()


class BinaryRpcProxy(object):
  def __init__(self, address, namespace='', src='', dst='',
               ioloop=None, encoding='utf-8',
               max_buffer_size=DEFAULT_MAX_BUFFER_SIZE,
               read_chunk_size=DEFAULT_READ_CHUNK_SIZE,
               ssl_options=None, timeout=DEFAULT_TIMEOUT):
    self._address = isinstance(address, str) and address or tuple(address)
    self._src = src
    self._dst = dst
    self._namespace = namespace
    self._ioloop = ioloop or rpclib.ioloop()
    self._encoding = encoding
    self._max_buffer_size = max_buffer_size
    self._read_chunk_size = read_chunk_size
    self._ssl_options = ssl_options
    self._timeout = timeout or DEFAULT_TIMEOUT
    self._name = None
    self._request_sent = False
    self._connected = False
    self._packer = Packer(encoding=encoding)
    self._result = NO_RESULT_EXCEPTION
    self._hotpotato = None
    self._input_binary = None

  def __getattr__(self, name):
    if name.startswith('_'):
      return object.__getattribute__(self, name)
    self._name = name
    return self

  def __repr__(self):
    return 'BinaryRpcProxy(' + ', '.join([str(self._address, self._namespace,
                                          str(self._name), self._dst)]) + ')'

  def __call__(self, *args, **kwargs):
    if not self._name:
      return Future(value=Exception("method name is not defined!"))

    streams = [s for s in args if isinstance(s, Binary)]
    if len(streams) == 1:
      self._input_binary = streams[0]
    elif len(streams) > 1:
      return Future(
          value=InvalidParameterError("Binary can't be set more than one"))

    self._args = [generalize(x) for x in args]
    self._kwargs = dict(
        [(k, generalize(v)) for k, v in kwargs.items() if not k.startswith("_")])

    try:
      self._stream = create_stream(self._address, self._ioloop,
                                   self._max_buffer_size,
                                   self._read_chunk_size,
                                   self._ssl_options)
      if self._stream:
        self._stream.set_close_callback(self._on_close)
      else:
        return Future(value=IOError("Fail to create stream address:%s",
                                    str(self._address)))
    except Exception, e:
      logging.error("Error while creating stream")
      return Future(value=e)

    request_type = BINARY_REQUEST

    binary_size = self._input_binary.size \
        if self._input_binary is not None else 0

    self._request = [
        request_type,
        self._src,
        self._dst,
        self._timeout,
        self._namespace,
        self._name,
        self._args,
        self._kwargs,
        binary_size
    ]
    logging.debug("request binary rpc namespace:%s, name:%s, address:%s",
                  str(self._namespace), str(self._name), str(self._address))
    self._future = Future()
    self._ioloop.add_callback(self._send_request)
    return self._future

  def _on_timeout(self):
    logging.error("RPC proxy timed out. namespace:%s, name:%s",
                  str(self._namespace), str(self._name))
    self._result = TimeoutError("Rpc timeout")
    self._fire_callback()
    self._stream.close()

  def _fire_callback(self):
    if self._future:
      self._future.set_value(self._result)

  def _on_close(self):
    pass

  def _send_request(self):
    try:
      self._hotpotato = HotPotato(self._ioloop, self._timeout,
                                  self._on_timeout)
      self._hotpotato.start()
      self._stream.connect(self._address, self._on_connected)
    except Exception, e:
      logging.exception("Failed to connect")
      self._result = e
      self._fire_callback()

  def _on_connected(self):
    self._connected = True
    try:
      self._transportor = SocketDataTransportor(self._stream)
      self._transportor.write(self._request).then(self._on_request_sent)
      if self._input_binary:
        write_stream = WriteStream(self._transportor.stream, self._ioloop,
                                   self._hotpotato, self._input_binary.size)
        self._input_binary._set_stream(write_stream, False)
    except Exception, e:
      logging.exception("Failed to write data, name:%s, address:%s",
                        str(self._name), str(self._address))
      self._result = e
      self._fire_callback()
      self._stream.close()

  def _on_request_sent(self, _):
    self._hotpotato.stop()
    self._request_sent = True
    self._hotpotato.start()
    self._transportor.read().then(self._handle_callback)

  def _handle_callback(self, result):
    self._hotpotato.stop()
    if not self._connected:
      self._result = IOError("Failed to connect the stream")
      self._fire_callback()
    elif self._future:
      try:
        _, _, result = result
        if not isinstance(result, Exception):
          result = specialize(result)
          binary = None
          if isinstance(result, _RpcError):
            result = result.convert()
          elif isinstance(result, Binary):
            binary = result
          elif isinstance(result, list):
            for value in result:
              if isinstance(value, Binary):
                binary = value
                break
          if binary:
            logging.debug("binary set!")
            binary._set_stream(ReadStream(self._stream, self._ioloop,
                                          self._hotpotato, binary.size),
                               True)
      finally:
        should_close = False
        if not binary:
          if self._input_binary:
            should_close = True
          else:
            result = Exception(
                "Request or response binary not exists for binary rpc.")
            self._stream.close()
        else:
          self._hotpotato.start()

        self._result = result
        self._fire_callback()
        self._input_binary._set_should_close(should_close)
    else:
      logging.debug("Closing the stream in proxy. No callback defined.")


class SharedRpcProxy(object):

  __WAITING__ = {}
  __RESPONSES__ = {}
  __STREAMS__ = {}

  @classmethod
  def dispose_all(cls):
    for address in cls.__STREAMS__.keys():
      cls.dispose(address)

    cls.__WAITING__.clear()
    cls.__RESPONSES__.clear()

  @classmethod
  def dispose(cls, address):
    rpc = cls.__STREAMS__.get(address)
    if rpc:
      stream, _ = rpc
      if stream and not stream.closed():
        stream.close()
      del cls.__STREAMS__[address]
    else:
      logging.debug("stream to dispose not exists for address:%s",
                    str(address))

  @classmethod
  def create(cls, address, namespace='', src='', dst='', ioloop=None,
             encoding='utf-8', ssl_options=None,
             max_buffer_size=DEFAULT_MAX_BUFFER_SIZE,
             read_chunk_size=DEFAULT_READ_CHUNK_SIZE, timeout=DEFAULT_TIMEOUT):
    # This function is not thread safe
    address = isinstance(address, str) and address or tuple(address)
    ioloop = ioloop or rpclib.ioloop()

    future = Future()
    rpc = cls.__STREAMS__.get(address)
    if rpc and rpc[0].closed():
      del cls.__STREAMS__[address]
      rpc = None

    try:
      if rpc:
        stream, reader = rpc
        new_rpc = cls(stream, address, reader, src, dst, namespace, ioloop,
                      timeout)
        return Future(value=new_rpc)
      else:
        if address not in cls.__WAITING__:
          stream = create_stream(address, ioloop, max_buffer_size,
                                 read_chunk_size, ssl_options)

          def connected():
            reader = Reader(stream, ioloop)
            reader.read()
            new_rpc = cls(stream, address, reader, src, dst, namespace, ioloop,
                          timeout)
            cls.__STREAMS__[address] = (stream, reader)
            for f in cls.__WAITING__[address]:
              f.set_value(new_rpc)
            del cls.__WAITING__[address]
          stream.connect(address, connected)
    except Exception, e:
      return Future(value=e)

    waiting = cls.__WAITING__.get(address)
    if waiting:
      waiting.append(future)
    else:
      cls.__WAITING__[address] = [future]

    return future

  def __init__(self, stream, address, reader, src='', dst='',
               namespace='', ioloop=None, timeout=DEFAULT_TIMEOUT):
    self._stream = stream
    self._address = address
    self._reader = reader
    self._src = src
    self._dst = dst
    self._namespace = namespace
    self._ioloop = ioloop or rpclib.ioloop()
    self._timeout = timeout or DEFAULT_TIMEOUT

  def __repr__(self):
    return 'SharedRpcProxy(' + ', '.join([str(self._address, self._namespace,
                                          str(self._src), self._dst)]) + ')'

  def __getattr__(self, name):
    if name.startswith('_'):
      return object.__getattribute__(self, name)
    return SharedRpcProxyMethod(self._stream, self._reader, self._namespace,
                                name, self._src, self._dst, self._ioloop,
                                self._timeout)


class SharedRpcProxyMethod(object):
  def __init__(self, stream, reader, namespace, name, src, dst, ioloop,
               timeout):
    self._stream = stream
    self._reader = reader
    self._namespace = namespace
    self._name = name
    self._src = src
    self._dst = dst
    self._ioloop = ioloop
    self._timeout = timeout

  def __call__(self, *args, **kwargs):
    if not self._name:
      return Future(value=Exception("method name is not defined!"))

    if self._stream.closed():
      return Future(value=Exception("Shared stream is closed"))

    streams = [s for s in args if isinstance(s, Binary)]
    if len(streams) > 0:
      return Future(value=InvalidParameterError('Binary is not supported'))

    self._args = [generalize(x) for x in args]
    self._kwargs = dict(
        [(k, generalize(v)) for k, v in kwargs.items() if not k.startswith("_")])

    request_type = SHARED_REQUEST

    response_id = str(uuid.uuid4())
    self._hotpotato = HotPotato(self._ioloop, self._timeout,
                                functools.partial(self._on_timeout,
                                                  response_id))
    self._request = [
        request_type,
        self._src,
        self._dst,
        self._timeout,
        self._namespace,
        self._name,
        self._args,
        self._kwargs,
        response_id,
    ]
    logging.debug("request shared rpc namespace:%s, name:%s",
                  str(self._namespace), str(self._name))
    future = Future()
    self._reader.add_callback(response_id, future)
    self._ioloop.add_callback(self._send_request)
    return future

  def _on_timeout(self, response_id):
    callback = self._reader.get_callback(response_id)
    if callback:
      callback.set_value(TimeoutError("Rpc timeout"))
      self._reader.remove_callback(response_id)

  def _send_request(self):
    self._hotpotato.start()
    SocketDataTransportor(self._stream).write(
        self._request).then(self._on_request_sent)

  def _on_request_sent(self, _):
    self._hotpotato.stop()


class MultiRpcProxy(object):
  pass


class Reader(object):
  def __init__(self, stream, ioloop):
    self._stream = stream
    self._ioloop = ioloop
    self._transportor = SocketDataTransportor(stream)
    self._responses = {}

  def add_callback(self, response_id, callback):
    self._responses[response_id] = callback

  def get_callback(self, response_id):
    return self._responses.get(response_id)

  def remove_callback(self, response_id):
    if response_id in self._responses:
      del self._responses[response_id]

  def read(self):
    self._ioloop.add_callback(self._read_data)

  def _read_data(self):
    if not self._transportor.closed():
      self._transportor.read().then(self._handle_callback)
    else:
      logging.warn("Stream in reader is closed")

  def _fire_callback(self, response_id, result):
    try:
      callback = self._responses.get(response_id)
      callback.set_value(result)
    except:
      logging.warn("Fail to handle callback of response id:%s",
                   str(response_id), exc_info=True)

  def _handle_callback(self, result):
    if isinstance(result, list) and len(result) == 3:
      _, response_id, result = result
      binary = None
      if response_id in self._responses:
        try:
          if not isinstance(result, Exception):
            result = specialize(result)
            if isinstance(result, _RpcError):
              result = result.convert()
            elif isinstance(result, Binary):
              binary = result
            elif isinstance(result, list):
              for value in result:
                if isinstance(value, Binary):
                  binary = value
                  break
            if binary:
              result = Exception("Binary is not supported")
        finally:
          self._fire_callback(response_id, result)
          self.read()
          del self._responses[response_id]
      else:
        logging.warn("No callback defined for response id:%s",
                     str(response_id))
        self.read()


def sync(method):
  """Synchronize the given RPC method.
    If given timeout, calls the given proxy method at the timeout seconds.
  """
  return _SyncRpcProxyMethod(method, rpclib.new_ioloop())


class _SyncRpcProxyMethod(object):
  def __init__(self, method, ioloop):
    self._method = copy.copy(method)
    self._method._ioloop = ioloop
    self._result = None

  def cancel(self):
    method = self._method
    self._method = None
    if method:
      method._ioloop.stop()
      rpclib.close_ioloop(method._ioloop)

  def __call__(self, *args, **kwargs):
    # TODO: support file stream by synchronously ans add test case.
    def callback(r):
      self._result = r
      self.cancel()
    future = self._method(*args, **kwargs)
    future.then(callback)
    self._method._ioloop.start()
    if isinstance(self._result, Exception):
      raise self._result
    elif isinstance(self._result, Binary):
      raise NotImplementedError("Not supported output strteam by "
                                "synchronously yet.")
    return self._result


class RpcProxyCollection(object):
  """Collection of RPC proxies to invoke simultaneously. All proxies must have
  the same RPC method defined.

  Usages::

    proxies = RpcProxyCollection([address1, address2])
    proxies.samefunc('param', _callback=values_callback)

  """
  def __init__(self, addresses, src='', dst='', namespace='',
               ioloop=None, encoding='utf-8',
               max_buffer_size=DEFAULT_MAX_BUFFER_SIZE,
               read_chunk_size=DEFAULT_READ_CHUNK_SIZE,
               ssl_options=None, timeout=DEFAULT_TIMEOUT):
    self._addresses = addresses
    self._namespace = namespace
    self._src = src
    self._dst = dst
    self._namespace = namespace
    self._ioloop = ioloop or rpclib.ioloop()
    self._encoding = encoding
    self._max_buffer_size = max_buffer_size
    self._read_chunk_size = read_chunk_size
    self._ssl_options = ssl_options
    self._timeout = timeout or DEFAULT_TIMEOUT

  def __getattr__(self, name):
    if name.startswith('_'):
      return object.__getattribute__(self, name)
    methods = []
    for address in self._addresses:
      proxy = SingleRpcProxy(address, self._namespace,
                             self._src, self._dst,
                             self._ioloop, self._encoding,
                             self._max_buffer_size,
                             self._read_chunk_size,
                             self._ssl_options,
                             self._timeout)
      proxy._name = name
      methods.append(proxy)
    return _RpcProxyCollectionMethod(methods)


class _RpcProxyCollectionMethod(object):
  def __init__(self, methods, condition=None):
    self._methods = methods
    self._results = [None] * len(methods)
    self._response_count = 0
    # termination condition.
    self._condition = condition
    self._future = Future()

  def cancel(self):
    for method in self._methods:
      method.cancel()

  def __call__(self, *args, **kwargs):
    def response(index, result):
      self._results[index] = result
      self._response_count += 1
      if self._response_count >= len(self._methods) or \
         self._condition and self._condition(result):
        if self._future:
          f = self._future
          self._future = None
          f.set_value(self._results)
          if self._condition and self._response_count < len(self._methods):
            # For early termination, cancel the left-overs.
            self.cancel()
        else:
          logging.debug("Rpc collection terminated earlier.")
    for index in range(len(self._methods)):
      method_f = self._methods[index](*args, **kwargs)
      method_f.get_value(functools.partial(response, index))
    return self._future


def greedy(collection_method, condition):
  """Set the greedy condition on the given `RpcCollectionProxy` method for
  early termination.
  """
  greedy_one = _RpcProxyCollectionMethod(collection_method._methods, condition)
  return greedy_one


class _RpcError(Base):
  """RPC error type (internal-only).
  """
  def __init__(self, msg=''):
    Base.__init__(self)
    self.msg = msg

  def convert(self):
    return RpcError(self.msg)


class RpcError(Exception):
  """RPC error type
  """
  def convert(self):
    return _RpcError(self.message)


class _NameSpaceNotFoundError(_RpcError):
  """Raised the request namespace is not found (internal-only)
  """
  def convert(self):
    return NameSpaceNotFoundError(self.msg)


class NameSpaceNotFoundError(RpcError):
  """Raised the request namespace is not found
  """
  def convert(self):
    return _NameSpaceNotFoundError(self.message)


class _RpcMethodNotFoundError(_RpcError):
  """Raised the request RPC method is not found (internal-only).
  """
  def convert(self):
    return RpcMethodNotFoundError(self.msg)


class RpcMethodNotFoundError(RpcError):
  """Raised the request RPC method is not found.
  """
  def convert(self):
    return _RpcMethodNotFoundError(self.message)


class _DestinationNotFoundError(_RpcError):
  """Raised the destination of rpc request is not found (internal-only).
  """
  def convert(self):
    return DestinationNotFoundError(self.msg)


class DestinationNotFoundError(RpcError):
  """Raised the destination of rpc request is not found
  """
  def convert(self):
    return _DestinationNotFoundError(self.message)


class _InvalidDestinationError(_RpcError):
  """Raised the destination of rpc request is not found (internal-only).
  """
  def convert(self):
    return InvalidDestinationError(self.msg)


class InvalidDestinationError(RpcError):
  """Raised the destination of rpc request is not found
  """
  def convert(self):
    return _InvalidDestinationError(self.message)


class NotGeneralizableError(Exception):
  pass


class _InvalidParameterError(_RpcError):
  """Raised the request parameter is invalid
  """
  def convert(self):
    return InvalidParameterError(self.msg)


class InvalidParameterError(RpcError):
  """Raised the request parameter is invalid
  """
  def convert(self):
    return _InvalidParameterError(self.message)


class _TimeoutError(_RpcError):
  """Raised the request is timed out (internal-only)
  """
  def convert(self):
    return TimeoutError(self.msg)


class TimeoutError(RpcError):
  """Raised the request is timed out
  """
  def convert(self):
    return _TimeoutError(self.message)


class _StreamClosedError(_RpcError):
  """Raised the request's stream is closed (internal-only)
  """
  def convert(self):
    return StreamClosedError(self.msg)


class StreamClosedError(RpcError):
  """Raised the request's stream is closed
  """
  def convert(self):
    return _StreamClosedError(self.message)


def create_socket(address, family=socket.AF_INET):
  """Helper to create a socket.
  """
  if isinstance(address, str):
    logging.debug("create unix socket")
    af = socket.AF_UNIX
    sock = socket.socket(af, socket.SOCK_STREAM)
  else:
    ip, port = address
    res = socket.getaddrinfo(ip, port, family, socket.SOCK_STREAM,
                             0, socket.AI_PASSIVE)[0]
    af, socktype, proto, canonname, sockaddr = res
    sock = socket.socket(af, socktype, proto)

  auto.set_close_exec(sock.fileno())
  sock.setblocking(0)
  if af == socket.AF_INET6:
    if hasattr(socket, "IPPROTO_IPV6"):
      sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
  return sock


def create_stream(address, ioloop, max_buffer_size=DEFAULT_MAX_BUFFER_SIZE,
                  read_chunk_size=DEFAULT_READ_CHUNK_SIZE, ssl_options=None):
  socket = create_socket(address)
  if not socket:
    raise IOError("Fail to create socket address:%s", str(address))

  if ssl_options:
    logging.debug("ssl io stream created")
    return SSLIOStream(socket, io_loop=ioloop,
                       max_buffer_size=max_buffer_size,
                       read_chunk_size=read_chunk_size,
                       ssl_options=ssl_options)
  else:
    logging.debug("nomal io stream created")
    return IOStream(socket, io_loop=ioloop,
                    max_buffer_size=max_buffer_size,
                    read_chunk_size=read_chunk_size)
