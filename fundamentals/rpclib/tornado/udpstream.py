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


"""UDP stream with similar APIs as the `IOStream`
"""

import logging
import rpclib
import time
import socket
import errno
from rpclib.tornado.ioloop import IOLoop


class UdpStream(object):
  """UDP stream to send/receive UDP messages.
  Strictly speaking, this is not a stream (UDP is not a streaming protocol).
  But the name is as-is for the sake of APIs.
  """
  BUFFER_SIZE = 4096

  def __init__(self, socket, ioloop=None):
    self.socket = socket
    self._state = None
    self._receive_callback = None
    self._ioloop = ioloop or rpclib.ioloop()

  def _add_io_state(self, state):
    if self._state is None:
      self._state = IOLoop.ERROR | state
      self._ioloop.add_handler(
          self.socket.fileno(), self._handle_events, self._state)
    elif not self._state & state:
      self._state = self._state | state
      self._ioloop.update_handler(self.socket.fileno(), self._state)

  def sendto(self, msg, address):
    """Send the msg to the UDP socket.
    """
    return self.socket.sendto(msg, address)

  def receive(self, receive_callback, timeout=None):
    if self._receive_callback:
      raise Exception("UPD is already being listened by some other callback.")
    self._receive_callback = receive_callback
    self._do_read()
    self._read_timeout = timeout and \
        self._ioloop.add_timeout(time.time() + timeout, self._timedout) or None
    self._add_io_state(self._ioloop.READ)

  def _do_read(self):
    while self.socket:
      try:
        data, address = self.socket.recvfrom(UdpStream.BUFFER_SIZE)
        self._receive_callback(data, address)
      except socket.error, e:
        if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
          return
        else:
          logging.exception("UDP socket error.")
          self.close()
          return

  def _timedout(self):
    if self._receive_callback:
      # TODO: close socket? error?
      self._receive_callback(None, None)

  def close(self):
    self._ioloop.remove_handler(self.socket.fileno())
    self.socket.close()
    self.socket = None

  def _handle_read(self):
    if self._read_timeout:
      self._ioloop.remove_timeout(self._read_timeout)
      self._read_timeout = None
    self._do_read()

  def _handle_events(self, fd, events):
    if events & self._ioloop.READ:
      self._handle_read()
    if events & self._ioloop.ERROR:
      logging.error("UPD socket event error. evt=%s", str(events))


def create_udp_stream(address, ioloop=None):
  """Creates an UDP stream on the given address.
  """
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
  sock.setblocking(False)
  sock.bind(address)
  return UdpStream(sock, ioloop)


def broadcast(msg, port):
  """Broadcast the message.
  """
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.setblocking(False)
  s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
  s.sendto(msg, ('<broadcast>', port))
  # socket is automatically closed when garbage-collected.
  # No need to close it here.