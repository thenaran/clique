#
# Copyright 2012 Narantech Inc.

# This program is a property of Narantech Inc. Any form of infringement is
# strictly prohibited. You may not, but not limited to, copy, steal, modify
# and/or redistribute without appropriate permissions under any circumstance.
#
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|


"""Helper for testing.
"""


class Restorer(object):
  """Helper for mocking and restoring module functions.

  Usage::

    def setUp(self):
      self.res = Restorer()
      self.res.mock(engine.led, 'breathe', mock_breathe)
      self.res.mock(engine.mq, 'publish', mock_publish)
      self.res.mock(engine.mq, 'subscribe', mock_subscribe)

    def tearDown(self):
      self.res.restore()

  """
  def __init__(self):
    self._funcs = {}

  def mock(self, mod, func_name, mock_func):
    """Mock the given function with the mock function.
    """
    func = getattr(mod, func_name)
    if func in self._funcs:
      raise Exception("Function %s has been mocked already." % str(func))
    setattr(mod, func_name, mock_func)
    self._funcs[func] = (mod, func_name)

  def restore(self):
    """Restore all original functions.
    """
    for func, (mod, func_name) in self._funcs.items():
      setattr(mod, func_name, func)
