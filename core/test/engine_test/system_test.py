# -*- coding: utf-8 -*-
# Copyright 2012-2013 Narantech Inc. All rights reserved.
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|
#


"""Test cases for system module
"""


from nose.plugins.attrib import attr

from rpclib.testing import RpcTestCase
import engine.system


@attr(species="clique", genus="core", family="engine", name="system")
class SystemTestCase(RpcTestCase):
  def test_get_main_memory(self):
    stats = engine.system.get_main_memory()
    self.assertTrue(4, len(stats))

  def test_cpu_percentage(self):
    stats = engine.system.get_cpu_percentage()
    self.assertTrue(len(stats) > 1)

  def test_get_disk_usage(self):
    total, used = engine.system.get_disk_usage()
    self.assertTrue(total > 1)
    self.assertTrue(used > 1)
