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


"""Clique gpio module.
"""
import cStringIO
import aengel
import logging
from engine.runtime import CLIQUE_USER

__AVAILABLE_PINS__ = set([4, 17, 22, 18, 23, 24, 25])  #GPIO2's pin number is different as board revisions


def export(pin, direction, edge=None):

  logging.debug("export pin:%s, direction:%s", str(pin), str(direction))
  if pin not in __AVAILABLE_PINS__:
    raise Exception("Not support pin number:%s", str(pin))

  #TODO(hdkim) : check direction
  output = cStringIO.StringIO()
  output.write("echo %s > /sys/class/gpio/export;" % str(pin))
  output.write("echo %s > /sys/class/gpio/gpio%s/direction;" % (str(direction),
                                                                str(pin)))
  output.write("chown %s:%s /sys/class/gpio/gpio%s/value;" %
               (str(CLIQUE_USER), str(CLIQUE_USER), str(pin)))
  if edge:
    if edge == 'falling' or edge == 'rising' or edge == 'both':
      output.write("echo %s > /sys/class/gpio/gpio%s/edge;" % (str(edge),
                                                               str(pin)))
    else:
      logging.warn("invalid edge:%s", str(edge))
  aengel.execute_cmd(output.getvalue())


def unexport(pin):
  logging.debug("unexport pin:%s", str(pin))
  if pin not in __AVAILABLE_PINS__:
    raise Exception("Not support pin number:%s", str(pin))

  output = cStringIO.StringIO()
  output.write("echo %s > /sys/class/gpio/unexport" % str(pin))
  aengel.execute_cmd(output.getvalue())
