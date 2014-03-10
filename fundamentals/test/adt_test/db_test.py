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

"""Test DB
"""
import os
from unittest import TestCase
from adt.db import DoubleKeyDict
from nose.plugins.attrib import attr


@attr(species="clique", genus="fundamentals", family="adt", name="doublekeydic")
class DoubleKeyDictTestCase(TestCase):
  def setUp(self):
    TestCase.setUp(self)
    self._db = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'db')

  def tearDown(self):
    TestCase.tearDown(self)
    if os.path.exists(self._db):
      os.remove(self._db)

  def test_setitem(self):
    key1 = "key1"
    key2 = "key2"
    data = 1

    dd = DoubleKeyDict(int, self._db)
    dd[key1, key2] = data

    self.assertEqual(dd[key1], (key1, key2, data))
    self.assertEqual(dd[key2], (key1, key2, data))

    all_items = dd._get_all_items()
    self.assertTrue(dd[key1] in all_items)
    self.assertTrue(dd[key2] in all_items)

  def test_delitem(self):
    key1 = "key1"
    key2 = "key2"
    data = 1

    dd = DoubleKeyDict(int, self._db)
    dd[key1, key2] = data

    self.assertEqual(dd[key1], (key1, key2, data))
    self.assertEqual(dd[key2], (key1, key2, data))

    all_items = dd._get_all_items()
    self.assertTrue(dd[key1] in all_items)
    self.assertTrue(dd[key2] in all_items)

    del dd[key1]
    self.assertFalse(key1 in dd)
    self.assertFalse(key2 in dd)

    all_items = dd._get_all_items()
    self.assertTrue(len(all_items) == 0)

  def test_contains(self):
    key1 = "key1"
    key2 = "key2"
    data = 1

    dd = DoubleKeyDict(int, self._db)
    dd[key1, key2] = data

    self.assertEqual(dd[key1], (key1, key2, data))
    self.assertEqual(dd[key2], (key1, key2, data))

    self.assertTrue(key1 in dd)
    self.assertTrue(key2 in dd)

  def test_iter(self):
    key1 = "key1"
    key2 = "key2"
    data = 1

    dd = DoubleKeyDict(int, self._db)
    dd[key1, key2] = data

    self.assertEqual(dd[key1], (key1, key2, data))
    self.assertEqual(dd[key2], (key1, key2, data))

    all_items = dd._get_all_items()
    import logging
    logging.debug("allitem:%s", str(all_items))
    for item in dd:
      self.assertTrue(dd[item] in all_items)

  def test_db_init(self):
    key1 = "key1"
    key2 = "key2"
    data = 1

    dd = DoubleKeyDict(int, self._db)
    dd[key1, key2] = data

    self.assertEqual(dd[key1], (key1, key2, data))
    self.assertEqual(dd[key2], (key1, key2, data))

    new = DoubleKeyDict(int, self._db)
    all_items = new._get_all_items()
    self.assertTrue(new[key1] in all_items)
    self.assertTrue(new[key2] in all_items)
