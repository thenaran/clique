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


""" test for first module.
"""
import os

from unittest import TestCase
from nose.plugins.attrib import attr
import engine.runtime
import engine.identity
import engine.runtime


@attr(species="clique", genus="core", family="engine", name="first")
class FirstTests(TestCase):
  def setUp(self):
    TestCase.setUp(self)

  def tearDown(self):
    TestCase.tearDown(self)

  def test_generate_certificates(self):
    engine.first.generate_rsa_keys()

    self.assertTrue(os.path.exists(engine.identity.__DATA__.cert_path))
    self.assertTrue(os.path.exists(engine.identity.__DATA__.key_path))
    self.assertTrue(os.path.exists(engine.identity.__DATA__.ca_certs))
    self.assertFalse(os.path.exists(engine.identity.__ROOT_KEY_FILENAME__))
    self.assertFalse(os.path.exists(engine.identity.__ROOT_CERT_FILENAME__))
