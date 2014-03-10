# -*- coding: utf-8 -*-
# Copyright 2012-2013 Narantech Inc. All rights reserved.
#  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
# |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
# |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
# |       |       |   |_||_|       |       | |   | |   |___|       |       |
# |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
# | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
# |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|


"""Test Config
"""


import os
from unittest import TestCase
from adt.config import JsonSettings
from nose.plugins.attrib import attr


JSON_SAMPLE1 = """
{
"engine":
    {
    "value":
        {
        "name":
            {
            "value": "name"
            },
        "ident":
            {
            "value": ""
            },
        "version":
            {
            "value": ""
            },
        "updated_date":
            {
            "value": 0
            }
        }
    }
}
"""

JSON_SAMPLE2 = """
{
"engine":
    {
    "value":
        {
        "name":
            {
            "value": null
            },
        "ident":
            {
            "value": "ident"
            },
        "version":
            {
            "value": "version"
            },
        "updated_date":
            {
            "value": 1
            }
        }
    }
}
"""


@attr(species="clique", genus="fundamentals", family="adt", name="JsonSettings")
class JsonSettingsTestCase(TestCase):
  def setUp(self):
    TestCase.setUp(self)
    self._sample1 = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'sample1')
    with open(self._sample1, 'w') as wf:
      wf.write(JSON_SAMPLE1)
    self._sample2 = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'sample2')
    with open(self._sample2, 'w') as wf:
      wf.write(JSON_SAMPLE2)
    self._sample3 = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'sample3')

  def tearDown(self):
    TestCase.tearDown(self)
    if os.path.exists(self._sample1):
      os.remove(self._sample1)
    if os.path.exists(self._sample2):
      os.remove(self._sample2)
    if os.path.exists(self._sample3):
      os.remove(self._sample3)

  def test_update_value_symbol(self):
    settings = JsonSettings([self._sample1, self._sample2])
    self.assertTrue('engine.name' in settings._config_symbol)
    self.assertTrue('engine.ident' in settings._config_symbol)
    self.assertTrue('engine.version' in settings._config_symbol)
    self.assertTrue('engine.updated_date' in settings._config_symbol)

  def test_set(self):
    settings = JsonSettings([self._sample1, self._sample2])
    key = 'engine.name'
    value = 'new_name'
    settings.set(key, value)
    self.assertEqual(settings._config_symbol['engine.name']['value'], value)
    self.assertEqual(settings._config_symbol['engine.name']['value'],
                     settings._config['engine']['value']['name']['value'])

  def test_set_new_key(self):
    settings = JsonSettings([self._sample1, self._sample2])
    key = 'engine.new'
    value = 'new_test_value'
    settings.set(key, value)
    self.assertEqual(settings._config_symbol['engine.new']['value'], value)
    self.assertEqual(settings._config_symbol['engine.new']['value'],
                     settings._config['engine']['value']['new']['value'])

  def test_flush(self):
    settings = JsonSettings([self._sample1, self._sample2])
    key = 'engine.name'
    old_value = settings.get(key)
    value = 'new_name'
    settings.set(key, value)
    settings.flush()
    old_settings = JsonSettings([self._sample1])
    self.assertEqual(old_settings._config_symbol['engine.name']['value'],
                     old_value)
    new_settings = JsonSettings([self._sample2])
    self.assertEqual(new_settings._config_symbol['engine.name']['value'],
                     value)

  def test_flush_specified_file(self):
    settings = JsonSettings([self._sample1, self._sample2])
    key = 'engine.name'
    old_value = settings.get(key)
    value = 'new_name'
    settings.set(key, value)
    settings.flush(self._sample3)
    new_settings = JsonSettings([self._sample3])
    self.assertEqual(new_settings._config_symbol['engine.name']['value'],
                     value)
    old_settings = JsonSettings([self._sample1, self._sample2])
    self.assertEqual(old_settings._config_symbol['engine.name']['value'],
                     old_value)
