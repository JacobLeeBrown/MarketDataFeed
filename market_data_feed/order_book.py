# market_data_feed/order_book.py
# original author: Jacob Brown
#
#
# Class for maintaining pertinent data for outputting inside bid and ask levels from the CoinBase Pro Market Feed API.

import json
import logging
from decimal import Decimal as D


class OrderBook:

    def __init__(self,
                 max_levels=5,
                 logging_enabled=False):
        # Feature for space efficiency -> only track the best <max_levels> levels
        # Also has minor impact on time efficiency for whenever the levels need to be sorted
        self.max_levels = max_levels

        self.logging_enabled = logging_enabled

        # Dict<String, Pair<Decimal, Set<String>>>
        # {Level Price : ( Level Quantity , { Order Ids } ) }
        # Ex: {"100.00" : ( 1.5, {"a1", "b2", "c3"} ) }
        self.best_ask_levels = {}
        self.best_bid_levels = {}

        # Dict<String, Pair<String, Decimal>>
        # {Order Id : ( Order Price , Order Quantity ) }
        # Ex: {"a1" : ( "100.00", 0.5 ) }
        self.ask_ids = {}
        self.bid_ids = {}

        self.worst_ask_price = D('-1.0')
        self.worst_bid_price = D('-1.0')

    def handle_event(self, event):
        if 'type' not in event:
            if self.logging_enabled:
                logging.debug('Event does not have \'type\' key: ' + json.dumps(event, indent=4))
            return

        event_type = event['type']
        if event_type == 'received':
            self._received(event)
        elif event_type == 'open':
            self._open(event)
        elif event_type == 'done':
            self._done(event)
        elif event_type == 'match':
            self._match(event)
        elif event_type == 'change':
            self._change(event)
        elif event_type == 'activate':
            self._activate(event)
        else:
            if self.logging_enabled:
                logging.debug('Unrecognized event type: ' + event_type)

    # Event Type Handlers

    def _received(self, event):
        pass  # Do nothing for `received` events in this project

    def _open(self, event):

        order_id = event['order_id']
        order_side = event['side']
        order_price = event['price']
        order_price_float = D(order_price)
        order_size = D(event['remaining_size'])

        if 'sell' == order_side:
            if order_price not in self.best_ask_levels:

                if len(self.best_ask_levels) < self.max_levels:
                    # Our book is not full, so simply add the new level and update class variables
                    self.best_ask_levels[order_price] = (order_size, {order_id})
                    self.ask_ids[order_id] = (order_price, order_size)
                    if self.worst_ask_price < order_price_float:
                        self.worst_ask_price = order_price_float

                elif self.worst_ask_price > order_price_float:
                    # Our book is full but the new level is better than our worst, so add it and remove the worst
                    self.best_ask_levels[order_price] = (order_size, {order_id})
                    self.ask_ids[order_id] = (order_price, order_size)

                    sorted_ask_prices = sorted(self.best_ask_levels.keys())
                    to_remove_price = sorted_ask_prices[-1]
                    self.worst_ask_price = D(sorted_ask_prices[-2])

                    # Remove orders from ask_ids dictionary for the level we will be removing, then remove that level
                    for order_id in self.best_ask_levels[to_remove_price][1]:
                        del self.ask_ids[order_id]
                    del self.best_ask_levels[to_remove_price]

            else:
                (quantity, order_ids) = self.best_ask_levels[order_price]
                order_ids.add(order_id)
                self.best_ask_levels[order_price] = (quantity + order_size, order_ids)
                self.ask_ids[order_id] = (order_price, order_size)

        elif 'buy' == order_side:
            if order_price not in self.best_bid_levels:

                if len(self.best_bid_levels) < self.max_levels:
                    # Our book is not full, so simply add the new level and update class variables
                    self.best_bid_levels[order_price] = (order_size, {order_id})
                    self.bid_ids[order_id] = (order_price, order_size)
                    if (self.worst_bid_price > order_price_float) or (self.worst_bid_price == D("-1.0")):
                        self.worst_bid_price = order_price_float

                elif self.worst_bid_price < order_price_float:
                    # Our book is full but the new level is better than our worst, so add it and remove the worst
                    self.best_bid_levels[order_price] = (order_size, {order_id})
                    self.bid_ids[order_id] = (order_price, order_size)

                    sorted_bid_prices = sorted(self.best_bid_levels.keys())
                    to_remove_price = sorted_bid_prices[0]
                    self.worst_bid_price = D(sorted_bid_prices[1])

                    # Remove orders from bid_ids dictionary for the level we will be removing, then remove that level
                    for order_id in self.best_bid_levels[to_remove_price][1]:
                        del self.bid_ids[order_id]
                    del self.best_bid_levels[to_remove_price]

            else:
                (quantity, order_ids) = self.best_bid_levels[order_price]
                order_ids.add(order_id)
                self.best_bid_levels[order_price] = (quantity + order_size, order_ids)
                self.bid_ids[order_id] = (order_price, order_size)

    def _done(self, event):

        order_id = event['order_id']
        order_side = event['side']

        if ('sell' == order_side) and (order_id in self.ask_ids):
            self._remove_sell_order(order_id)

        if ('buy' == order_side) and (order_id in self.bid_ids):
            self._remove_buy_order(order_id)

    def _match(self, event):

        order_id = event['maker_order_id']  # Only care about maker id since that's the resting order
        order_side = event['side']
        order_size = D(event['size'])

        if ('sell' == order_side) and (order_id in self.ask_ids):
            (price, quantity) = self.ask_ids[order_id]
            if quantity == order_size:
                # Maker order was completely filled -> remove from book
                self._remove_sell_order(order_id)
            else:
                # Maker order was partially filled -> adjust respective data structures
                self._adjust_sell_order(order_id, D(order_size))

        elif ('buy' == order_side) and (order_id in self.bid_ids):
            (price, quantity) = self.bid_ids[order_id]
            if quantity == order_size:
                # Maker order was completely filled -> remove from book
                self._remove_buy_order(order_id)
            else:
                # Maker order was partially filled -> adjust respective data structures
                self._adjust_buy_order(order_id, D(order_size))

    def _change(self, event):
        pass  # TODO: While change orders are important, in practice they essentially never occur -> deprioritize

    def _activate(self, event):
        pass  # Do nothing for `activate` events in this project

    # Helpers

    def _remove_sell_order(self, order_id):
        (price, quantity) = self.ask_ids[order_id]
        (level_quantity, level_ids) = self.best_ask_levels[price]

        level_ids.remove(order_id)

        if len(level_ids) == 0:
            # This is only order in level -> Remove entire level and update worst level if needed
            sorted_ask_prices = sorted(self.best_ask_levels.keys())
            if D(price) == self.worst_ask_price:
                if len(sorted_ask_prices) == 1:
                    # This is only ask level -> reset globals
                    self.worst_ask_price = D("-1.0")
                else:
                    self.worst_ask_price = D(sorted_ask_prices[-2])
            del self.best_ask_levels[price]
        else:
            level_quantity -= quantity
            self.best_ask_levels[price] = (level_quantity, level_ids)

        del self.ask_ids[order_id]

    def _remove_buy_order(self, order_id):
        (price, quantity) = self.bid_ids[order_id]
        (level_quantity, level_ids) = self.best_bid_levels[price]

        level_ids.remove(order_id)

        if len(level_ids) == 0:
            # This is only order in level -> Remove entire level and update worst level if needed
            sorted_bid_prices = sorted(self.best_bid_levels.keys())
            if D(price) == self.worst_bid_price:
                if len(sorted_bid_prices) == 1:
                    # This is only bid level -> reset globals
                    self.worst_bid_price = D("-1.0")
                else:
                    self.worst_bid_price = D(sorted_bid_prices[1])
            del self.best_bid_levels[price]
        else:
            level_quantity -= quantity
            self.best_bid_levels[price] = (level_quantity, level_ids)

        del self.bid_ids[order_id]

    def _adjust_sell_order(self, order_id, quantity_delta):
        (price, quantity) = self.ask_ids[order_id]
        self.ask_ids[order_id] = (price, quantity - quantity_delta)

        (level_quantity, level_ids) = self.best_ask_levels[price]
        self.best_ask_levels[price] = (level_quantity - quantity_delta, level_ids)

    def _adjust_buy_order(self, order_id, quantity_delta):
        (price, quantity) = self.bid_ids[order_id]
        self.bid_ids[order_id] = (price, quantity - quantity_delta)

        (level_quantity, level_ids) = self.best_bid_levels[price]
        self.best_bid_levels[price] = (level_quantity - quantity_delta, level_ids)
