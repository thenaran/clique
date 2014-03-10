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


"""Concurrent and parallel programming library.
"""


import logging
import time
import contextlib
import subprocess
from Queue import Queue
from threading import Thread
from functools import partial
from cStringIO import StringIO
from subprocess import PIPE, STDOUT, Popen
from adt.llist import dllist


class TaskHandle(object):
  """The TaskHandle is responsible for handling task and progress

  usage:
    TaskHandle = TaskQueue().do(Task).then(Task).then(Task)

    progress:

    TaskHandle.set_progress_handler(handler)

      set progress handler to TaskHandle to receive updated progress
      to receive sumerized progress, should set progress handler to last task handle

    cancel:

    TaskHandle.cancel()

      to cancel task, call cancel method of not completed task.
      if cancel method of completed task is called, the cancel will be ignored

    rollback:

      if rollback is True, the recover method of task will be called when task has error or canceled
      if rollback is True, when error occurs on current task, the next run method will be ignored although stop_on_error is False
      if you set rollback, rollback of prev task will be set but not next task
  """
  def __init__(self, task, ioloop=None, rollback=False):
    self._value = Future()
    self._task = task
    self._task.set_progress_handler(self._handle_progress)
    self._handlers = []
    self._prev_task_handle = None
    self._ioloop = ioloop
    self._current_progress = 0
    self._rollback = rollback
    self._canceled = False
    self._error = None

  @property
  def rollback(self):
    return self._rollback

  @rollback.setter
  def rollback(self, value):
    self._rollback = value

    if self._prev_task_handle:
      self._prev_task_handle.rollback = value

  @property
  def progress(self):
    return self._current_progress

  def _set_task_handle(self, handle):
    self._prev_task_handle = handle

  def _pass_progress(self, progress):
    progress = (progress >> 1) + (self._current_progress >> 1)

    for handler in self._handlers:
      handler(progress)

    if self._has_next():
      self._next._pass_progress(progress)

  def _handle_progress(self, progress):
    self._current_progress = progress

    if self._prev_task_handle:
      progress = (progress >> 1) + (self._prev_task_handle.progress >> 1)

    for handler in self._handlers:
      handler(progress)

    if self._has_next():
      self._next._pass_progress(progress)

  def set_progress_handler(self, handler):
    self._handlers.append(handler)

  def cancel(self):
    self._canceled = True

    if self._prev_task_handle:
      self._prev_task_handle.cancel()

  def recover(self):
    # if don't has value, don't need to recover
    if not self._error:
      self._task.recover()

    if self._prev_task_handle:
      self._prev_task_handle.recover()

  @property
  def value(self):
    return self._value

  def then(self, task):
    self._next = TaskHandle(task, self._ioloop)
    self._next._set_task_handle(self)
    self._handle_next()
    return self._next

  def __call__(self, *args, **kwargs):
    if self._canceled:
      self._error = Exception("the task is canceled")
      self._handle_next()
    else:
      if self._ioloop:
        self._ioloop.add_callback(self._do_call, *args, **kwargs)
      else:
        self._do_call(*args, **kwargs)

  def _do_call(self, *args, **kwargs):
    try:
      if not self._task._completed:
        v = self._task.run(*args, **kwargs)
        if isinstance(v, Future):
          def callback(result):
            if isinstance(result, Exception):
              self._error = result
              self._handle_next()
            else:
              self._set_result(result)
            self._task._completed = True
          v.then(callback)
        else:
          if isinstance(v, Exception):
            self._error = v
            self._handle_next()
          else:
            self._set_result(v)
          self._task._completed = True
      else:
        self._set_result(Exception("task is already completed"))
    except Exception, e:
      logging.exception("Error while executing task")
      self._error = e
      self._handle_next()

  def _pass_result(self, result):
    self._value.set_value(result)

    if self._has_next():
      self._next._pass_result(result)

  def _set_result(self, result):
    self._value.set_value(result)
    self._handle_next()

  def _handle_next(self):
    if self._ioloop:
      self._ioloop.add_callback(self._do_handle_next)
    else:
      self._do_handle_next()

  def _do_handle_next(self):
    if self._error:
      if self._rollback:
        self.recover()

      self._pass_result(self._error)

    elif self._has_next() and self._value.has_value():
      self._next(self._value._value)

  def _has_next(self):
    return hasattr(self, '_next')


class Task(object):
  def __init__(self, description=''):
    self._progress_handler = None
    self._completed = False
    self._description = description

  @property
  def description(self):
    return self._description

  def set_progress_handler(self, handler):
    self._progress_handler = handler

  def set_progress(self, progress):
    if self._progress_handler:
      self._progress_handler(progress)

  def run(self, *args, **kwargs):
    pass

  def recover(self):
    pass


class TaskQueue(object):
  """Admin-worker task queue for executing background jobs.
  All members are non-blocking.
  """

  def __init__(self, queue_size, pool_size, ioloop=None):
    self.queue_size = queue_size
    self.pool_size = pool_size
    self.running = False
    self.marker = object()
    self.ioloop = ioloop

  def start(self):
    if self.running:
      logging.warn("TaskQueue is already executing.")
      return
    self.running = True
    self.queue = Queue(self.queue_size)
    for i in range(self.pool_size):
      Thread(target=self._do_callback).start()

  def stop(self):
    if not self.running:
      logging.warn("TaskQueue is already not running.")
      return
    self.running = False
    logging.debug("TaskQueue stopping.....")
    for i in range(self.pool_size):
      self.queue.put_nowait((self.marker, None, None))
    self.queue.join()
    logging.debug("TaskQueue terminated.")
    self.queue = None

  def do(self, callback, *args, **kwargs):
    """Do the callback task in background.
    """
    f = Future()
    item = (callback, f, args, kwargs)
    self.queue.put_nowait(item)
    return f

  def _do_callback(self):
    while True:
      callback, future, args, kwargs = self.queue.get()
      try:
        if callback == self.marker:
          break
        try:
          value = callback(*args, **kwargs)
          if isinstance(value, Future):
            value.then(future.set_value)
          else:
            future.set_value(value)
        except Exception, e:
          future.set_value(e)
      except:
        logging.exception("Error executing the callback.")
      finally:
        self.queue.task_done()

  def WIP_do(self, task, *args, **kwargs):
    """run the task in background.
    """
    h = TaskHandle(task, self.ioloop)
    item = (h, args, kwargs)
    self.queue.put_nowait(item)
    return h

  def WIP_do_callback(self):
    while True:
      h, args, kwargs = self.queue.get()
      try:
        if h == self.marker:
          break
        h(*args, **kwargs)
      except:
        logging.exception("Error executing the callback.")
      finally:
        self.queue.task_done()


class Future(object):
  """Abstract data that might not exist at the time of invocation and become realized some time in the future.

  For example::

    >>> v = a.get_async()
    >>> v.then(handle_a).then(handle_b).then(handle_c)

  If handle_a returns a Future, then handle_b receives the value set to the
  returned Future.
  """
  MARKER = object()

  def __init__(self, value=MARKER, ioloop=None, stop_on_error=False):
    self._callbacks = []
    self._ioloop = ioloop
    self._stop_on_error = stop_on_error
    if value != Future.MARKER:
      self._value = value

  def set_value(self, value):
    if self.has_value():
      raise Exception("The value of future can't be set twice")
    self._value = value
    for callback, future in self._callbacks:
      if self._ioloop:
        self._ioloop.add_callback(self._handle_callback, callback, future,
                                  value)
      else:
        self._handle_callback(callback, future, value)

  def then(self, value_callback):
    """Gets the value asynchronously. The value will be returned through the
    given callback as an argument.
    """
    if self.has_value():
      logging.debug("has value, value callback called!!")
      try:
        v = value_callback(self._value)
      except Exception, e:
        if not self._stop_on_error:
          v = e
        else:
          logging.info("Error in callback.", exc_info=True)
          raise e
      logging.debug("value_callback result:%s", str(v))
      return v if isinstance(v, Future) else Future(value=v)
    else:
      f = Future()
      self._callbacks.append((value_callback, f))
      return f
  get_value = then  # for compatiblity

  def has_value(self):
    return hasattr(self, '_value')

  def _handle_callback(self, callback, future, value):
    """Helper to handle the set value.
    """
    try:
      v = callback(value)
    except Exception, e:
      if not self._stop_on_error:
        future.set_value(e)
      else:
        logging.info("Error in callback.", exc_info=True)
    else:
      if isinstance(v, Future):
        v.then(lambda v: (not isinstance(v, Exception) or
                          not self._stop_on_error) and future.set_value(v))
      else:
        future.set_value(v)

  @classmethod
  def throw_if_error(cls, func):
    """Throw the given error if one of the function argument is an exception.
    """
    def wrapped(*args, **kwargs):
      for arg in args:
        if isinstance(arg, Exception):
          raise arg
      for arg in kwargs.itervalues():
        if isinstance(arg, Exception):
          raise arg
      return func(*args, **kwargs)
    return wrapped


class FutureCollection(object):
  """Collection of :class: Future

     Example::

       >>> fc = FutureCollection(future1, future2, future3)
       >>> fc.one().then(callback)
       >>> fc.all().then(callback)
       >>> fc.until(lambda value: value is None).then(callback)
  """
  def __init__(self, *futures):
    self.futures = futures

  def one(self):
    #TODO: Need to implemented
    raise Exception("Not implemented")

  def all(self):
    return self.all_but_until()

  def all_but_until(self, condition=None):
    if len(self.futures) == 0:
      return Future(None)
    future = Future()
    values = [None] * len(self.futures)

    counter = Lazy()
    counter.count = 0

    def ret_cb(i, ret):
      if not future.has_value():
        values[i] = ret
        if condition and condition(ret) is True:
          future.set_value(values)

        counter.count += 1
        if counter.count >= len(self.futures):
          future.set_value(values)

    for i in range(len(self.futures)):
      self.futures[i].then(partial(ret_cb, i))

    return future

  def until(self, condition):
    future = Future()

    counter = Lazy()
    counter.count = 0

    def ret_cb(ret):
      counter.count += 1
      if not future.has_value():
        if condition(ret) is True:
          future.set_value(ret)
        elif counter.count >= len(self.futures):
          future.set_value(None)

    for f in self.futures:
      f.then(ret_cb)

    return future


class Lazy(object):
  """Abstract data that will be initialized when it is actually accessed.
  """
  def __init__(self):
    self._inits = {}

  def add_initializer(self, name, initializer):
    """Adds the initializer function to the specified property.
    """
    self._inits[name] = initializer

  def __getattr__(self, name):
    try:
      return object.__getattr__(self, name)
    except:
      initializer = self._inits[name]
      value = initializer()
      object.__setattr__(self, name, value)
      return value

  def __delattr__(self, name):
    try:
      return object.__delattr__(self, name)
    except:
      logging.debug("Failed to delete the attribute %s", name, exc_info=True)
    else:
      logging.debug("Deleted the attribute %s from Lazy.", name)

  def __setattr__(self, name, value):
    return object.__setattr__(self, name, value)


class HotPotato(object):
  """Time is runnin out!!!
  """
  def __init__(self, ioloop, timeout, on_timeout):
    self._ioloop = ioloop
    self._timeout = timeout
    self._on_timeout = on_timeout
    self._timeout_obj = None

  def set_timeout(self, timeout):
    self._timeout = timeout

  def start(self):
    self._ioloop.add_callback(self._start)

  def stop(self):
    self._ioloop.add_callback(self._stop)

  def _start(self):
    self._stop()
    self._timeout_obj = \
        self._ioloop.add_timeout(time.time() + self._timeout, self._on_timeout)

  def _stop(self):
    if self._timeout_obj:
      self._ioloop.remove_timeout(self._timeout_obj)
      self._timeout_obj = None


class BufferedQueue(object):
  """ Store item into double linked list
  """
  def __init__(self, condition, values=None):
    self._buffer = dllist(values or [])
    self._condition = condition

  def append(self, item):
    """ append item to buffer

    appends item to left and remove item with checking condition.
    remove type is FIFO

    Example:::
      >>> def condition(q):
      >>>   return q.size <= 3
      >>>
      >>> bq = BufferQueue(condition)
      >>> bq.append(1)   #[1]
      >>> bq.append(2)   #[2, 1]
      >>> bq.append(3)   #[3, 2, 1]
      >>> bq.append(4)   #[4, 3, 2]

    Or:::
      >>> def condition(q):
      >>>   return q.first.value - q.last.value <= 5
      >>>
      >>> bq = BufferQueue(condition)
      >>> bq.append(10)   #[10]
      >>> bq.append(13)   #[13, 10]
      >>> bq.append(15)   #[15, 13, 10]
      >>> bq.append(17)   #[17, 15, 13]
    """
    self._buffer.appendleft(item)
    self._maintain()

  def _maintain(self):
    """ checking buffer values with condition
    remove type is FIFO
    """
    try:
      while not self._condition(self._buffer):
        self._buffer.pop()
    except ValueError:
      # buffer is empty
      pass

  def collect(self, condition=None):
    """ collect items from buffer

    get lists on left if false by check condition from right
    collect type is LIFO

    Example:::
      >>>
      >>> bq
      [35, 24, 13, 1, 11, 3]
      >>> bq.collect(lambda x: x > 10)
      [35, 24, 13]
    """
    collect_list = []
    try:
      for item in self._buffer:
        if condition(item):
          collect_list.append(item)
        else:
          break

    except:
      # buffer is empty
      pass
    finally:
      return collect_list

  def clear(self):
    self._buffer.clear()


class Timer(object):
  """ Callback timer

  Examle:::
    >>> def hello():
    >>>   print 'hello'
    >>> Timer(ioloop, 5, hello)
    ... after 5 sec
    >>>
    hello

  Or:::
    >>> def hello():
    >>>   print 'hello'
    >>> Timer(ioloop, 5, hello, True)
    ... after 5 sec
    >>>
    hello
    ... after 5 sec
    >>>
    hello
  """

  def __init__(self, ioloop, interval, callback, repeat=False):
    self._ioloop = ioloop
    self._interval = interval
    self._callback = callback
    self._is_repeat = repeat
    self._is_canceled = False
    self._timeout = self._ioloop.add_timeout(time.time() + self._interval,
                                             self._run_callback)

  def _run_callback(self):
    if not self._is_canceled:
      self._ioloop.add_callback(self._callback)
      if self._is_repeat:
        self._timeout = self._ioloop.add_timeout(time.time() + self._interval,
                                                 self._run_callback)

  def cancel(self):
    self._ioloop.remove_timeout(self._timeout)
    self._is_canceled = True

  def refresh(self):
    self._ioloop.remove_timeout(self._timeout)
    self._timeout = self._ioloop.add_timeout(time.time() + self._interval,
                                             self._run_callback)


class CommandExecuter(object):
  """ Specified command executor with Thread
  """
  # Unix, Windows and old Macintosh end-of-line
  _newlines = ['\n', '\r\n', '\r']

  def __init__(self, executers):
    self._executers = executers
    self._proc = None

  def do(self, cmd, output_callback=None, process_callback=None, level=0):
    self._cmd = cmd
    self._callback = output_callback
    self._future = Future()
    self._executers.add(self)
    self._process_callback = process_callback
    self._run_level = level
    Thread(target=self._run).start()
    return self._future

  def _unbuffered(self, proc, stream='stdout'):
    stream = getattr(proc, stream)
    with contextlib.closing(stream):
      while True:
        out = StringIO()
        last = stream.read(1)
        # Don't loop forever
        if last == '' and proc.poll() is not None:
          break
        while last not in self._newlines:
          # Don't loop forever
          if last == '' and proc.poll() is not None:
            break
          out.write(last)
          last = stream.read(1)
        yield out.getvalue()
        out.close()

  def _run(self):
    try:
      self._proc = Popen(self._cmd, stdout=PIPE, stderr=STDOUT,
                         universal_newlines=True, shell=True)
      if self._run_level != 0:
        prior_cmd = 'renice {level} {pid}'.format(
            level=self._run_level, pid=self._proc.pid)
        subprocess.check_call(prior_cmd, shell=True)
      if self._callback:
        for line in self._unbuffered(self._proc):
          self._callback(line)
      else:
        self._proc.wait()
      ret = self._proc.returncode
    except Exception as e:
      if self._callback:
        self._callback(e)
      else:
        logging.exception("Fail to execute command '%s'", self._cmd)
      ret = e
    finally:
      self._executers.discard(self)
      self._proc = None
      self._future.set_value(ret)
