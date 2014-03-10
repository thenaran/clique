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


"""Test cases for network
"""


from nose.plugins.attrib import attr

from rpclib.testing import RpcTestCase
import engine.network


@attr(species="clique", genus="core", family="engine", name="network")
class NetworkTestCase(RpcTestCase):
  def test_get_link_stats(self):
    stats = engine.network.get_link_stats()
    self.assertTrue(2, len(stats))
