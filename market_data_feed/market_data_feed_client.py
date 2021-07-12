# market_data_feed/market_data_feed_client.py
# original author: Jacob Brown
#
#
# Wrapper class around WebSocketClient to pull in OrderBook logic and expose relevant data access methods. Inspired by
# example from original websocket_client.py in coinbasepro-python project:
# https://github.com/danpaquin/coinbasepro-python/blob/master/cbpro/websocket_client.py

import sys
import time
import copy
import logging
from . import websocket_client as wc
from .order_book import OrderBook
from decimal import Decimal as D


class MarketDataFeedClient(wc.WebSocketClient):

    def __init__(self,
                 level_count=5,  # Number of inside levels to output
                 max_levels=15,  # Number of inside levels to track in the OrderBook, must be greater than level_count
                 logging_enabled=False):
        assert(max_levels >= level_count)
        super().__init__()
        self.level_count = level_count
        self.order_book = OrderBook(max_levels=max_levels)
        self.logging_enabled = logging_enabled

        # Statistics
        self.message_type_count = {'subscriptions': 0,
                                   'received': 0,
                                   'open': 0,
                                   'done': 0,
                                   'match': 0,
                                   'change': 0,
                                   'activate': 0}
        self.total_message_count = 0

    def on_open(self):
        self.url = "wss://ws-feed.pro.coinbase.com/"
        self.products = ["BTC-USD"]
        self.channels = ["full"]

    def on_message(self, msg):
        if 'type' in msg:
            self.message_type_count[msg['type']] += 1
            self.order_book.handle_event(msg)
            if self.logging_enabled:
                logging.debug(self.get_inside_levels_printout(self.level_count) + "\n")
        self.total_message_count += 1

    def get_inside_levels_printout(self, level_count):
        best_ask_levels = copy.deepcopy(self.order_book.best_ask_levels)
        best_bid_levels = copy.deepcopy(self.order_book.best_bid_levels)

        sorted_ask_level_prices = sorted(best_ask_levels.keys())
        sorted_bid_level_prices = sorted(best_bid_levels.keys(), reverse=True)

        best_ask_levels = self._get_best_levels(sorted_ask_level_prices, best_ask_levels, level_count)
        best_bid_levels = self._get_best_levels(sorted_bid_level_prices, best_bid_levels, level_count)

        return self._format_inside_levels(best_ask_levels, best_bid_levels)

    @staticmethod
    def _get_best_levels(sorted_prices, level_map, level_count):
        levels = []
        quantity_precision = 5
        price_precision = 2
        for i in range(level_count):
            if i >= len(sorted_prices):
                pass  # Edge case when book first opens
            else:
                target_price = sorted_prices[i]
                target_price_decimal = D(target_price)
                levels.append((round(level_map[target_price][0], quantity_precision),
                               round(target_price_decimal, price_precision)))
        return levels

    @staticmethod
    def _format_inside_levels(best_ask_levels, best_bid_levels):
        longest_quantity_length = 0
        longest_price_length = 0

        for i in range(len(best_ask_levels)):
            cur_ask_quantity_len = len(str(best_ask_levels[i][0]))
            cur_ask_price_len = len(str(best_ask_levels[i][1]))
            cur_bid_quantity_len = len(str(best_bid_levels[i][0]))
            cur_bid_price_len = len(str(best_bid_levels[i][1]))

            longest_quantity_length = max(longest_quantity_length, cur_ask_quantity_len, cur_bid_quantity_len)
            longest_price_length = max(longest_price_length, cur_ask_price_len, cur_bid_price_len)

        # Begin constructing output string
        output = ''
        for i in range(len(best_ask_levels)):
            (quantity, price) = best_ask_levels[-(i+1)]
            padded_quantity = str(quantity).rjust(longest_quantity_length, ' ')
            padded_price = str(price).rjust(longest_price_length, ' ')
            output += ' ' + padded_quantity + ' @ ' + padded_price + '\n'

        output += ''.rjust(longest_quantity_length + longest_price_length + 5, '-') + '\n'

        for i in range(len(best_bid_levels)):
            (quantity, price) = best_bid_levels[i]
            padded_quantity = str(quantity).rjust(longest_quantity_length, ' ')
            padded_price = str(price).rjust(longest_price_length, ' ')
            output += ' ' + padded_quantity + ' @ ' + padded_price + '\n'

        return output[:-1]


# Main method to allow for direct interaction without API layer
def main():

    mdf_client = MarketDataFeedClient()
    mdf_client.start()

    try:
        while True:
            logging.info("###\n")
            logging.info(mdf_client.get_inside_levels_printout(5) + "\n")
            logging.info("\nTotal Messages = %i, Breakdown = %s\n" %
                         (mdf_client.total_message_count, str(mdf_client.message_type_count)))
            time.sleep(5)
    except KeyboardInterrupt:
        mdf_client.close()

    if mdf_client.error:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
