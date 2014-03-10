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

import logging
try:
  import hal.gpio as gpio
except:
  logging.warn("Fail to import gpio")
  gpio = None

INPUT = 0
OUTPUT = 1
PWM_OUTPUT = 2


def pinMode(pin, direction):
  gpio.pinMode(pin, direction)


def setup_sys():
  gpio.wiringPiSetupSys()


def setup():
  gpio.wiringPiSetup()


def digitalWrite(pin, value):
  gpio.digitalWrite(pin, value)


def digitalRead(pin):
  return gpio.digitalRead(pin)


def pwmWrite(pin, value):
  gpio.pwmWrite(pin, value)


def delayMicroseconds(microsec):
  gpio.delayMicroseconds(microsec)


def waitForInterruptSys(pin, ms):
  return gpio.waitForInterruptSys(pin, ms)

if not gpio:
  def noop(*args):
    pass
  pinMode = noop
  setup = noop
  digitalWrite = noop
  digitalRead = noop
  pwmWrite = noop
  delayMicroseconds = noop
  waitForInterruptSys = noop


class Gpio(object):
  def __init__(self, pin, direction):
    self._pin = pin

    setup_sys()
    direction = OUTPUT if direction == 'out' else INPUT
    logging.debug("init pin mode in pin:%s ,direction:%s",
                  str(pin), str(direction))
    pinMode(pin, direction)

  def write(self, value):
    digitalWrite(self._pin, value)

  def read(self):
    return digitalRead(self._pin)

  def pwmWrite(self, value):
    pwmWrite(self._pin, value)

  def waitForInterruptSys(self, ms):
    return waitForInterruptSys(self._pin, ms)
