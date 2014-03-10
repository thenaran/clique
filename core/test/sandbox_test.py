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


"""Sandbox environment tests.
"""


from unittest import TestCase


__SIMPLE_APP_ENTRY_FILE__ = '''simple_app.py'''
__COMPLEX_APP_ENTRY_FILE__ = '''complex_app.py'''
__BAD_APP_ENTRY_FILE__ = '''bad_app.py'''
__TEST_APP_NAME__ = '''sandboxtest'''


class SandboxTest(TestCase):
  """Sandbox integration tests.
  """
  @classmethod
  def setUpClass(cls):
    # Setup the test environment.
    # 1. create the test app user
    # 2. set up app data under /home/sandboxtest/ (setup the jail)
    # 3. copy the test apps to /home/sandboxtest/app/
    TestCase.setUpClass()

  @classmethod
  def tearDownClass(cls):
    # Clean up the test environment.
    # 1. delete the test app user with its home directory
    # 2. kill any left-over processes (use psutil to kill all children)
    TestCase.tearDownClass()

  def setUp(self):
    # Initialize the app module.
    TestCase.setUp(self)

  def tearDown(self):
    # Kill all test processes.
    TestCase.tearDown(self)

  def test_execute(self):
    """Test simple execution and termination.
    """
    pass

  def test_execute_same_apps(self):
    """Test the same application more than once.
    """
    pass

  def test_terminate_non_service(self):
    """Test termination on non-service, non-running app.
    """
    pass

  def test_terminate_complex_service(serf):
    """Test terminating a complicated service app with multiple processes
    under it.
    """
    pass

  def test_execute_no_app(self):
    """Test execution of non-existent app.
    """
    pass

  def test_corrupted_app(Self):
    """Test execution of corrupted/invalid app.
    """
    pass


if  __name__ == "__main__":
  from unittest import main
  main()
