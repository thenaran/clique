from __future__ import absolute_import, division, with_statement
from rpclib.tornado import netutil
from rpclib.tornado.ioloop import IOLoop
from rpclib.tornado.iostream import IOStream, SSLIOStream
from rpclib.tornado.log import gen_log, app_log
from rpclib.stackcontext import NullContext
from rpclib.testing import RpcTestCase, bind_unused_port, ExpectLog
from nose.plugins.attrib import attr
import errno
import os
import socket
import sys


class IOStreamMixin(object):
    def _make_server_iostream(self, connection, **kwargs):
        raise NotImplementedError()

    def _make_client_iostream(self, connection, **kwargs):
        raise NotImplementedError()

    def make_iostream_pair(self, **kwargs):
        listener, port = bind_unused_port()
        streams = [None, None]

        def accept_callback(connection, address):
            streams[0] = self._make_server_iostream(connection, **kwargs)
            if isinstance(streams[0], SSLIOStream):
                # HACK: The SSL handshake won't complete (and
                # therefore the client connect callback won't be
                # run)until the server side has tried to do something
                # with the connection.  For these tests we want both
                # sides to connect before we do anything else with the
                # connection, so we must cause some dummy activity on the
                # server.  If this turns out to be useful for real apps
                # it should have a cleaner interface.
                streams[0]._add_io_state(IOLoop.READ)
            self.stop()

        def connect_callback():
            streams[1] = client_stream
            self.stop()
        netutil.add_accept_handler(listener, accept_callback,
                                   io_loop=self._ioloop)
        client_stream = self._make_client_iostream(socket.socket(), **kwargs)
        client_stream.connect(('127.0.0.1', port),
                              callback=connect_callback)
        self.start(condition=lambda: all(streams))
        self._ioloop.remove_handler(listener.fileno())
        listener.close()
        return streams

    def test_write_zero_bytes(self):
        # Attempting to write zero bytes should run the callback without
        # going into an infinite loop.
        server, client = self.make_iostream_pair()
        server.write(b"", callback=self.stop)
        self.start()
        # As a side effect, the stream is now listening for connection
        # close (if it wasn't already), but is not listening for writes
        self.assertEqual(server._state, IOLoop.READ | IOLoop.ERROR)
        server.close()
        client.close()

    def test_connection_refused(self):
        # When a connection is refused, the connect callback should not
        # be run.  (The kqueue IOLoop used to behave differently from the
        # epoll IOLoop in this respect)
        server_socket, port = bind_unused_port()
        server_socket.close()
        stream = IOStream(socket.socket(), self._ioloop)
        self.connect_called = False

        def connect_callback():
            self.connect_called = True
        stream.set_close_callback(self.stop)
        # log messages vary by platform and ioloop implementation
        with ExpectLog(gen_log, ".*", required=False):
            stream.connect(("localhost", port), connect_callback)
            self.start()
        self.assertFalse(self.connect_called)
        self.assertTrue(isinstance(stream.error, socket.error), stream.error)
        if sys.platform != 'cygwin':
            # cygwin's errnos don't match those used on native windows python
            self.assertEqual(stream.error.args[0], errno.ECONNREFUSED)

    def test_gaierror(self):
        # Test that IOStream sets its exc_info on getaddrinfo error
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        stream = IOStream(s, io_loop=self._ioloop)
        stream.set_close_callback(self.stop)
        # To reliably generate a gaierror we use a malformed domain name
        # instead of a name that's simply unlikely to exist (since
        # opendns and some ISPs return bogus addresses for nonexistent
        # domains instead of the proper error codes).
        with ExpectLog(gen_log, "Connect error"):
            stream.connect(('an invalid domain', 54321))
            self.assertTrue(isinstance(stream.error, socket.gaierror), stream.error)

    def test_read_callback_error(self):
        # Test that IOStream sets its exc_info when a read callback throws
        server, client = self.make_iostream_pair()
        try:
            server.set_close_callback(self.stop)
            with ExpectLog(
                app_log, "(Uncaught exception|Exception in callback)"
            ):
                # Clear ExceptionStackContext so IOStream catches error
                with NullContext():
                    server.read_bytes(1, callback=lambda data: 1 / 0)
                client.write(b"1")
                self.start()
            self.assertTrue(isinstance(server.error, ZeroDivisionError))
        finally:
            server.close()
            client.close()

    def test_streaming_callback(self):
        server, client = self.make_iostream_pair()
        try:
            chunks = []
            final_called = []

            def streaming_callback(data):
                chunks.append(data)
                self.stop()

            def final_callback(data):
                self.assertFalse(data)
                final_called.append(True)
                self.stop()
            server.read_bytes(6, callback=final_callback,
                              streaming_callback=streaming_callback)
            client.write(b"1234")
            self.start(condition=lambda: chunks)
            client.write(b"5678")
            self.start(condition=lambda: final_called)
            self.assertEqual(chunks, [b"1234", b"56"])

            # the rest of the last chunk is still in the buffer
            server.read_bytes(2, callback=self.stop)
            data = self.start()
            self.assertEqual(data, b"78")
        finally:
            server.close()
            client.close()

    def test_delayed_close_callback(self):
        # The scenario:  Server closes the connection while there is a pending
        # read that can be served out of buffered data.  The client does not
        # run the close_callback as soon as it detects the close, but rather
        # defers it until after the buffered read has finished.
        server, client = self.make_iostream_pair()
        try:
            client.set_close_callback(self.stop)
            server.write(b"12")
            chunks = []

            def callback1(data):
                chunks.append(data)
                client.read_bytes(1, callback2)
                server.close()

            def callback2(data):
                chunks.append(data)
            client.read_bytes(1, callback1)
            self.start()  # stopped by close_callback
            self.assertEqual(chunks, [b"1", b"2"])
        finally:
            server.close()
            client.close()

    def test_close_buffered_data(self):
        # Similar to the previous test, but with data stored in the OS's
        # socket buffers instead of the IOStream's read buffer.  Out-of-band
        # close notifications must be delayed until all data has been
        # drained into the IOStream buffer. (epoll used to use out-of-band
        # close events with EPOLLRDHUP, but no longer)
        #
        # This depends on the read_chunk_size being smaller than the
        # OS socket buffer, so make it small.
        server, client = self.make_iostream_pair(read_chunk_size=256)
        try:
            server.write(b"A" * 512)
            client.read_bytes(256, self.stop)
            data = self.start()
            self.assertEqual(b"A" * 256, data)
            server.close()
            # Allow the close to propagate to the client side of the
            # connection.  Using add_callback instead of add_timeout
            # doesn't seem to work, even with multiple iterations
            self._ioloop.add_timeout(self._ioloop.time() + 0.01, self.stop)
            self.start()
            client.read_bytes(256, self.stop)
            data = self.start()
            self.assertEqual(b"A" * 256, data)
        finally:
            server.close()
            client.close()

    def test_inline_read_error(self):
        # An error on an inline read is raised without logging (on the
        # assumption that it will eventually be noticed or logged further
        # up the stack).
        server, client = self.make_iostream_pair()
        try:
            os.close(server.socket.fileno())
            with self.assertRaises(socket.error):
                server.read_bytes(1, lambda data: None)
        finally:
            server.close()
            client.close()

    def test_async_read_error_logging(self):
        # Socket errors on asynchronous reads should be logged (but only
        # once).
        server, client = self.make_iostream_pair()
        server.set_close_callback(self.stop)
        try:
            # Start a read that will be fullfilled asynchronously.
            server.read_bytes(1, lambda data: None)
            client.write(b"a")
            # Stub out read_from_fd to make it fail.

            def fake_read_from_fd():
                os.close(server.socket.fileno())
                server.__class__.read_from_fd(server)
            server.read_from_fd = fake_read_from_fd
            # This log message is from _handle_read (not read_from_fd).
            with ExpectLog(gen_log, "error on read"):
                self.start()
        finally:
            server.close()
            client.close()


@attr(species="clique", genus="fundamentals", family="rpclib", name="iostream")
class TestIOStream(IOStreamMixin, RpcTestCase):
    def _make_server_iostream(self, connection, **kwargs):
        return IOStream(connection, io_loop=self._ioloop, **kwargs)

    def _make_client_iostream(self, connection, **kwargs):
        return IOStream(connection, io_loop=self._ioloop, **kwargs)
