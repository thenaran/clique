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


""" Functools
Helper functions.
"""


import logging
from collections import deque


def replace(cond, obj, func, copy=False):
  """Helper to replace values under the given condition using the decorator
  function.

  Args:
    cond: condition function accepting key and value
    obj: list or dictionary or object
    func: decoration function accepting key and value
    copy: if True, objects, dictionaries, lists are shallow-copied.

  Returns:
    value itself or replaced value.
  """
  # Recursively replace objects
  queue = deque()
  result = {'': obj}
  queue.appendleft((result, '', obj))

  while len(queue) > 0:
    # dict or list or object, key or index, value
    d, k, v = queue.pop()
    if v is None:
      continue
    if isinstance(v, tuple):
      # convert to mutable list
      v = d[k] = list(v)
    elif copy:
      if isinstance(v, list):
        v = d[k] = list(v)
      elif hasattr(v, 'copy'):
        v = d[k] = v.copy()

    try:
      if cond(k, v):
        v = d[k] = func(k, v)
    except:
      pass

    if isinstance(v, dict):
      queue.extendleft([(v, k2, v2) for k2, v2 in v.items()])
    elif isinstance(v, list):
      zipped = zip(range(len(v)), v)
      queue.extendleft([(v, index, value) for index, value in zipped])
    elif hasattr(v, '__dict__'):
      queue.extendleft([(v.__dict__, k2, v2) for k2, v2 in v.__dict__.items()])

  # Return the adapted original obj_data
  return result['']


def adjust_number(value, minimum=None, maximum=None):
  """Adjusts the given number `value` according the `minimum` and `maximum`
  values.
  """
  if minimum and maximum:
    if minimum <= maximum:
      return min(max(value, minimum), maximum)
    else:
      raise Exception("Invalid min/max values")
  elif minimum:
    return max(value, minimum)
  elif maximum:
    return min(value, maximum)
  else:
    raise Exception("Either min or max value must be given.")


def try_execute(func, *args):
  """Try to execute the function. Any exception raised in the `func` is ignored.
  """
  try:
    return func(*args)
  except:
    logging.debug("error in try_execute", exc_info=True)


def valid_value(value, default):
  """validate value that the value is not None or Exception.
  if the value is None or Exception, default value will be returned.
  """
  if value is not None and not isinstance(value, Exception):
    return value

  return default
