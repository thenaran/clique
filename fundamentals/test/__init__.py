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


"""Test initiailization code.
"""

import logging
import sys


formatter = logging.Formatter("%(created)-13s\t%(levelname)8s\t \
  %(process)d\t%(thread)d\t%(module)s\t%(asctime)s\t%(funcName)s\t \
  %(lineno)d\t%(message)s")
logging.getLogger().setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(stream=sys.stderr)
stream_handler.setFormatter(formatter)
logging.getLogger().addHandler(stream_handler)
