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


""" Concurrency module tests.
"""

from adt.concurrency import Future
from adt.concurrency import BufferedQueue
from adt.concurrency import FutureCollection
from adt.concurrency import Timer
from nose.plugins.attrib import attr
from rpclib.testing import RpcTestCase
from rpclib.stackcontext import wrap


@attr(species="clique", genus="fundamentals", family="adt", name="future")
class FutureTests(RpcTestCase):
  """Future ADT tests.
  """
  def test_single_then(self):
    """Test single-depth then function.
    """
    f = Future()
    value = "abc"

    def callback(v):
      self.assertEqual(v, value)
      self.stop()

    f.then(callback)
    self.ioloop().add_callback(f.set_value, value)
    self.start()

  def test_two_depth_then(self):
    """Test two-depth then function.
    """
    f = Future()
    value1 = "abc"
    value2 = "123"

    def callback1(v1):
      self.assertEqual(v1, value1)
      return value2

    def callback2(v2):
      self.assertEqual(v2, value2)
      self.stop()

    f.then(callback1).then(callback2)
    self.ioloop().add_callback(f.set_value, value1)
    self.start()

  def test_future_then(self):
    """Test two-depth then function with the first one returning another
    :class:`Future`.
    """
    f = Future()
    value1 = "abc"
    value2 = "123"

    def callback1(v1):
      self.assertEqual(v1, value1)
      f2 = Future()
      self.ioloop().add_callback(f2.set_value, value2)
      return f2

    def callback2(v2):
      self.assertEqual(v2, value2)
      self.stop()

    f.then(callback1).then(callback2)
    self.ioloop().add_callback(f.set_value, value1)
    self.start()

  def test_mixed_future_then(self):
    """Test functions returning Futures and normal values at the same time.
    """
    f = Future()
    value1 = "abc"
    value2 = "123"
    value3 = "丑中之"

    def callback1(v1):
      self.assertEqual(v1, value1)
      f2 = Future()
      self.ioloop().add_callback(f2.set_value, value2)
      return f2

    def callback2(v2):
      self.assertEqual(v2, value2)
      return value3

    def callback3(v3):
      self.assertEqual(v3, value3)
      self.stop()

    f.then(callback1).then(callback2).then(callback3)
    self.ioloop().add_callback(f.set_value, value1)
    self.start()

  def test_future_multidepth_then(self):
    """Test multi-depth functions with only futures returned by each one.
    """
    f = Future()
    value1 = "abc"
    value2 = "123"
    value3 = "丑中之"

    def callback1(v1):
      self.assertEqual(v1, value1)
      f2 = Future()
      self.ioloop().add_callback(f2.set_value, value2)
      return f2

    def callback2(v2):
      self.assertEqual(v2, value2)
      f3 = Future()
      self.ioloop().add_callback(f3.set_value, value3)
      return f3

    def callback3(v3):
      self.assertEqual(v3, value3)
      f4 = Future()
      self.ioloop().add_callback(f4.set_value, None)
      return f4

    def callback4(v4):
      self.assertIsNone(v4)
      self.stop()

    f.then(callback1).then(callback2).then(callback3).then(callback4)
    self.ioloop().add_callback(f.set_value, value1)
    self.start()

  def test_none_then(self):
    """Test multi-depth functions returning only Nones.
    """
    f = Future()

    def callback(v):
      self.assertIsNone(v)

    def callback2(v):
      self.assertIsNone(v)
      self.stop()

    f.then(callback).then(callback).then(callback).then(callback2)
    self.ioloop().add_callback(f.set_value, None)
    self.start()

  def test_exception_then(self):
    """Test when an exception is thrown while execution.
    """
    f = Future()

    class CustomException(Exception):
      pass

    def callback(v):
      pass

    @wrap
    def callback2(v):
      raise CustomException("expected error")

    f.then(callback).then(callback2)
    self.ioloop().add_callback(f.set_value, None)
    self.assertRaises(CustomException, self.start)

  def test_throw_if_error(self):
    """ Tests when value is exception with throw_if_error decorator
    """
    e = Exception('error')

    @Future.throw_if_error
    def a(v):
      self.fail('Fail throw error')

    @Future.throw_if_error
    def b(v):
      self.fail('Fail throw error')

    def cb(v):
      self.assertIsInstance(Exception, v)
      self.assertEqual(e, v)
      self.stop()

    f = Future(e)
    f.then(a).then(b).then(cb)
    self.start()


@attr(species="clique", genus="fundamentals", family="adt", name="fc")
class FutureTests(RpcTestCase):
  def test_all(self):
    """Test when using several future with all fuction
    """
    f1 = Future()
    f2 = Future()
    f3 = Future()
    f4 = Future()

    e = [None] * 4
    e[0] = 0
    e[1] = 1
    e[2] = 2
    e[3] = 3

    def all_cb(ret):
      self.assertEqual(e[0], ret[0])
      self.assertEqual(e[1], ret[1])
      self.assertEqual(e[2], ret[2])
      self.assertEqual(e[3], ret[3])
      self.stop()

    FutureCollection(f1, f2, f3, f4).all().then(all_cb)

    self.ioloop().add_callback(f1.set_value, e[0])
    self.ioloop().add_callback(f3.set_value, e[2])
    self.ioloop().add_callback(f4.set_value, e[3])
    self.ioloop().add_callback(f2.set_value, e[1])
    self.start()

  def test_until(self):
    """Test when using several future with until function
    """
    f1 = Future()
    f2 = Future()
    f3 = Future()
    f4 = Future()

    e = [None] * 4
    e[0] = 0
    e[1] = 1
    e[2] = 2
    e[3] = 3

    def until_cb(ret):
      self.assertEqual(ret, 1)
      self.stop()

    FutureCollection(f1, f2, f3, f4).until(lambda r: r == 1).then(until_cb)

    self.ioloop().add_callback(f1.set_value, e[0])
    self.ioloop().add_callback(f3.set_value, e[2])
    self.ioloop().add_callback(f4.set_value, e[3])
    self.ioloop().add_callback(f2.set_value, e[1])
    self.start()


@attr(species="clique", genus="fundamentals", family="adt", name="buf")
class BufferedQueueTests(RpcTestCase):
  def test_collect(self):
    """Tests collect by condition
    """
    dq = BufferedQueue(lambda x: True, '987654321')

    e_list = ['9', '8', '7', '6']
    e_buffer = list('987654321')
    a_list = dq.collect(lambda x: int(x) > 5)
    a_buffer = list(dq._buffer)
    self.assertEquals(e_list, a_list)
    self.assertEquals(e_buffer, a_buffer)

  def test_collect_empty(self):
    """Tests collect by condition : buffer is empty
    """
    dq = BufferedQueue(lambda x: True)

    e_list = []
    e_buffer = []
    a_list = dq.collect(lambda x: int(x) > 5)
    a_buffer = list(dq._buffer)
    self.assertEquals(e_list, a_list)
    self.assertEquals(e_buffer, a_buffer)

  def test_collect_one_value_condition_true(self):
    """Tests collect by condition : buffer size is one and condition is true
    """
    dq = BufferedQueue(lambda x: True, '1')

    e_list = ['1']
    e_buffer = ['1']
    a_list = dq.collect(lambda x: int(x) > 0)
    a_buffer = list(dq._buffer)
    self.assertEquals(e_list, a_list)
    self.assertEquals(e_buffer, a_buffer)

  def test_collect_one_value_condition_false(self):
    """Tests collect by condition : buffer size is one and condition is false
    """
    dq = BufferedQueue(lambda x: True, '1')

    e_list = []
    e_buffer = ['1']
    a_list = dq.collect(lambda x: int(x) > 1)
    a_buffer = list(dq._buffer)
    self.assertEquals(e_list, a_list)
    self.assertEquals(e_buffer, a_buffer)

  def test_append(self):
    """Tests append by condition
    """
    dq = BufferedQueue(lambda x: (int(x.first.value) - int(x.last.value)) < 5,
                       '987654321')
    e_list = ['10', '9', '8', '7', '6']
    dq.append('10')
    a_list = list(dq._buffer)
    self.assertEquals(e_list, a_list)


@attr(species="clique", genus="fundamentals", family="adt", name="timer")
class TimerTests(RpcTestCase):
  def setUp(self):
    RpcTestCase.setUp(self)

  def tearDown(self):
    RpcTestCase.tearDown(self)

  def test_run(self):
    """Tests run
    """
    def test():
      self.stop()

    Timer(self._ioloop, 1, test)
    self.start()

  def test_repeat(self):
    """Tests repeat
    """
    c = []
    timer = None

    def test():
      c.append(1)
      if len(c) > 3:
        timer.cancel()
        self.stop()

    timer = Timer(self._ioloop, 1, test, True)
    self.start()

from adt.concurrency import TaskQueue
from adt.concurrency import Task
import logging


@attr(species="clique", genus="fundamentals", family="adt", name="taskqueue")
class TaskQueueTests(RpcTestCase):
  def setUp(self):
    RpcTestCase.setUp(self)
    self._queue = TaskQueue(10, 1, self.ioloop())
    self.old_call = TaskQueue._do_callback
    TaskQueue._do_callback = TaskQueue.WIP_do_callback
    self._queue.start()

  def tearDown(self):
    RpcTestCase.tearDown(self)
    TaskQueue._do_callback = self.old_call
    self._queue.stop()

  def test_error_on_then(self):
    arg = [1, '', b'']
    err = "error"

    class ErrorTask(Task):
      def run(self, i, s, b):
        logging.debug("run task")
        raise Exception(err)

    def callback(result):
      logging.debug("callback result:%s", str(result))
      self.assertTrue(isinstance(result, Exception))
      self.assertEqual(err, str(result))
      self.stop()

    self._queue.WIP_do(ErrorTask(), *arg).value.then(callback)
    self.start(2)

  def test_error_on_multi_then(self):
    arg = [1, '', b'']
    err = "error"
    test = self

    class ErrorTask(Task):
      def run(self, i, s, b):
        logging.debug("run task")
        raise Exception(err)

    class ErrorResult(Task):
      def run(self, error):
        test.assertEqual(str(error), err)
        return error

    class ErrorFutureResult(Task):
      def run(self, error):
        test.assertEqual(str(error), err)
        return Future(value=error)

    def callback(result):
      logging.debug("callback result:%s", str(result))
      self.assertTrue(isinstance(result, Exception))
      self.assertEqual(err, str(result))
      self.stop()

    self._queue.WIP_do(ErrorTask(), *arg).then(
        ErrorResult()).then(ErrorFutureResult()).value.then(callback)
    self.start(2)

  def test_result_on_multi_then(self):
    arg = [1, '', b'']
    test = self
    ret = 100
    ret2 = 200
    ret3 = 300

    class FirstTask(Task):
      def run(self, i, s, b):
        return ret

    class SecondResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        return Future(value=ret2)

    class ThirdResult(Task):
      def run(self, result):
        test.assertEqual(ret2, result)
        f = Future()
        def callback():
          f.set_value(ret3)
        test.ioloop().add_callback(callback)
        return f

    def callback(result):
      self.assertEqual(ret3, result)
      self.stop()

    self._queue.WIP_do(FirstTask(), *arg).then(
        SecondResult()).then(ThirdResult()).value.then(callback)
    self.start(2)

  def test_progress_handler(self):
    args = [1, '', b'']
    progresses = []
    ret = 100
    test = self

    class FirstTask(Task):
      def run(self, i, s, b):
        f = Future()

        def callback():
          self.set_progress(20)
          self.set_progress(40)
          self.set_progress(100)
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

    class SecondResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        f = Future()

        def callback():
          self.set_progress(20)
          self.set_progress(40)
          self.set_progress(100)
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

    class ThirdResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        self.set_progress(20)
        self.set_progress(40)
        self.set_progress(100)
        return result

    def progress(progress):
      logging.debug("last progress:%s", str(progress))
      progresses.append(progress)

    def callback(result):
      self.assertEqual(ret, result)
      self.assertEqual(len(progresses), 9)
      self.assertEqual(progresses[0], 5)
      self.assertEqual(progresses[1], 10)
      self.assertEqual(progresses[2], 25)
      self.assertEqual(progresses[3], 30)
      self.assertEqual(progresses[4], 35)
      self.assertEqual(progresses[5], 50)
      self.assertEqual(progresses[6], 60)
      self.assertEqual(progresses[7], 70)
      self.assertEqual(progresses[8], 100)
      self.stop()

    h = self._queue.WIP_do(FirstTask("first"), *args).then(SecondResult("second")).then(ThirdResult("third"))
    h.value.then(callback)
    h.set_progress_handler(progress)
    self.start(2)

  def test_cancel_result(self):
    args = [1, '', b'']
    ret = 10
    test = self

    class FirstTask(Task):
      def run(self, i, s, b):
        f = Future()

        def callback():
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

    class SecondResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        f = Future()
        cancel()

        def callback():
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

    class ThirdResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        return result

    def callback(result):
      self.assertTrue(isinstance(result, Exception))
      self.stop()

    h = self._queue.WIP_do(FirstTask(), *args).then(SecondResult()).then(ThirdResult())
    h.value.then(callback)

    def cancel():
      h.cancel()

    self.start(2)

  def test_error_result(self):
    args = [1, '', b'']
    ret = 100
    runs = []
    test = self

    class FirstTask(Task):
      def run(self, i, s, b):
        f = Future()

        def callback():
          f.set_value(Exception("error"))
        test.ioloop().add_callback(callback)
        return f

    class SecondResult(Task):
      def run(self, result):
        runs.append(1)
        test.assertEqual(ret, result)
        f = Future()

        def callback():
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

    class ThirdResult(Task):
      def run(self, result):
        runs.append(1)
        test.assertEqual(ret, result)
        return result

    def callback(result):
      self.assertTrue(isinstance(result, Exception))
      self.assertEqual(len(runs), 0)
      self.stop()

    h = self._queue.WIP_do(FirstTask(), *args).then(SecondResult()).then(ThirdResult())
    h.value.then(callback)

    self.start(2)

  def test_cancel_rollback(self):
    args = [1, '', b'']
    ret = 100
    test = self
    reverses = []

    class FirstTask(Task):
      def run(self, i, s, b):
        f = Future()

        def callback():
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

      def recover(self):
        reverses.append(1)

    class SecondResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        f = Future()
        cancel()

        def callback():
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

      def recover(self):
        reverses.append(1)

    class ThirdResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        return result

      def recover(self):
        reverses.append(1)

    def callback(result):
      logging.debug("callback result:%s", str(result))
      self.assertTrue(isinstance(result, Exception))
      self.assertEqual(len(reverses), 2)
      self.stop()

    h = self._queue.WIP_do(FirstTask('first'), *args).then(SecondResult('second')).then(ThirdResult('third'))
    h.rollback = True
    h.value.then(callback)

    def cancel():
      h.cancel()

    self.start(2)

  def test_error_rollback(self):
    args = [1, '', b'']
    ret = 100
    test = self
    reverses = []

    class FirstTask(Task):
      def run(self, i, s, b):
        f = Future()

        def callback():
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

      def recover(self):
        reverses.append(1)

    class SecondResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        f = Future()

        def callback():
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

      def recover(self):
        reverses.append(1)

    class ThirdResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        return Exception("error")

      def recover(self):
        reverses.append(1)

    def callback(result):
      logging.debug("callback result:%s", str(result))
      self.assertTrue(isinstance(result, Exception))
      self.assertEqual(len(reverses), 2)
      self.stop()

    h = self._queue.WIP_do(FirstTask('first'), *args).then(SecondResult('second')).then(ThirdResult('third'))
    h.rollback = True
    h.value.then(callback)

    self.start(2)

  def test_secondtask_error_rollback(self):
    args = [1, '', b'']
    ret = 100
    test = self
    reverses = []
    runs = []

    class FirstTask(Task):
      def run(self, i, s, b):
        f = Future()

        def callback():
          f.set_value(ret)
        test.ioloop().add_callback(callback)
        return f

      def recover(self):
        reverses.append(1)

    class SecondResult(Task):
      def run(self, result):
        test.assertEqual(ret, result)
        f = Future()

        def callback():
          f.set_value(Exception("error"))
        test.ioloop().add_callback(callback)
        return f

      def recover(self):
        reverses.append(1)

    class ThirdResult(Task):
      def run(self, result):
        runs.append(1)
        test.assertEqual(ret, result)
        return ret

      def recover(self):
        reverses.append(1)

    def callback(result):
      logging.debug("callback result:%s", str(result))
      self.assertTrue(isinstance(result, Exception))
      self.assertEqual(len(reverses), 1)
      self.assertEqual(len(runs), 0)
      self.stop()

    h = self._queue.WIP_do(FirstTask('first'), *args).then(SecondResult('second')).then(ThirdResult('third'))
    h.rollback = True
    h.value.then(callback)

    self.start(2)
