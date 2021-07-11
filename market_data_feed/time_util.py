# market_data_feed/time_util.py
# original author: Jacob Brown
#
#
# Util module for handling time-related features

import time


def current_milli_time():
    return round(time.time() * 1000)
