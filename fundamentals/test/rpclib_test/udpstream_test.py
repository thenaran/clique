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


"""UDP stream specific tests.
"""
import logging
from rpclib.testing import RpcTestCase
import rpclib.tornado.udpstream as udpstream
from rpclib.stackcontext import wrap
import random
from nose.plugins.attrib import attr
from threading import Thread


@attr(species="clique", genus="fundamentals", family="rpclib", name="udpstream")
class UdpStreamTests(RpcTestCase):
  """Unit-test the UdpStream class.
  """

  def test_multicast_simple(self):
    """Test simple multicast(broadcast); one server, one client.
    """
    # Server setup
    port = random.randint(30000, 40000)  # some random port
    address = ''  # empty address for broadcast
    server = udpstream.create_udp_stream((address, port), self.ioloop())
    message = 'hello'

    def receive_callback(data, address):
      logging.debug("Data received. data=%s from %s", data, address)
      self.assertEqual(message, data)
      self.stop()
      server.close()
    logging.info("Test UDP stream will listen to the port %d", port)
    server.receive(wrap(receive_callback))

    def do_send():
      try:
        udpstream.broadcast(message, port)
      except:
        self.set_error()
    self.ioloop().add_callback(lambda: Thread(target=do_send).start())
    self.start()

  def test_multicast_multi(self):
    """Test multicast; multiple servers, one client broadcasting.
    """
    # Server setup
    port = random.randint(30000, 40000)  # some random port
    address = ''  # empty address for broadcast
    message = 'hello'
    max_count = 3
    count = [0]
    servers = []

    @wrap
    def receive_callback(data, address):
      count[0] += 1
      logging.debug("receive callback #%d from %s", count[0], address)
      self.assertEqual(message, data)
      if count[0] >= max_count:
        self.stop()
        for server in servers:
          server.close()
    logging.info("Test UDP stream will listen to the port %d", port)

    for i in range(max_count):
      server = udpstream.create_udp_stream((address, port), self.ioloop())
      server.receive(receive_callback)
      servers.append(server)

    def do_send():
      try:
        udpstream.broadcast(message, port)
      except:
        self.set_error()
    self.ioloop().add_callback(lambda: Thread(target=do_send).start())
    self.start()
