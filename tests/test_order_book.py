import unittest
from market_data_feed import order_book as ob
from decimal import Decimal as D


class TestOrderBook(unittest.TestCase):

    ###################
    # Open Unit Tests #
    ###################

    def test_open_sell_new_price_levels(self):
        target = ob.OrderBook(max_levels=2)

        # Sub case: Completely empty order book

        event = {"type": "open", "order_id": "1", "remaining_size": "1.005", "price": "1.00", "side": "sell"}

        target.handle_event(event)

        expected_best_ask_levels = {"1.00": (D("1.005"), {"1"})}
        expected_best_bid_levels = {}
        expected_ask_ids = {"1": ("1.00", D("1.005"))}
        expected_bid_ids = {}
        expected_worst_ask_price = D("1.00")
        expected_worst_bid_price = D("-1.0")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

        # Sub case: Non-empty, non-full order book

        event = {"type": "open", "order_id": "2", "remaining_size": "0.5", "price": "2.00", "side": "sell"}

        target.handle_event(event)

        expected_best_ask_levels = {"1.00": (D("1.005"), {"1"}), "2.00": (D("0.5"), {"2"})}
        expected_best_bid_levels = {}
        expected_ask_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5"))}
        expected_bid_ids = {}
        expected_worst_ask_price = D("2.00")
        expected_worst_bid_price = D("-1.0")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_open_sell_same_price_levels(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {"1.00": (D("1.005"), {"1"}), "2.00": (D("0.5"), {"2"})}
        target.best_bid_levels = {}
        target.ask_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5"))}
        target.bid_ids = {}
        target.worst_ask_price = D("2.00")
        target.worst_bid_price = D("-1.0")

        # Open orders come in for existing price levels -> quantity should increase accordingly

        event = {"type": "open", "order_id": "3", "remaining_size": "1.005", "price": "1.00", "side": "sell"}
        target.handle_event(event)
        event = {"type": "open", "order_id": "4", "remaining_size": "0.5", "price": "2.00", "side": "sell"}
        target.handle_event(event)

        expected_best_ask_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("1.0"), {"2", "4"})}
        expected_best_bid_levels = {}
        expected_ask_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        expected_bid_ids = {}
        expected_worst_ask_price = D("2.00")
        expected_worst_bid_price = D("-1.0")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_open_sell_full_book(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("1.0"), {"2", "4"})}
        target.best_bid_levels = {}
        target.ask_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        target.bid_ids = {}
        target.worst_ask_price = D("2.00")
        target.worst_bid_price = D("-1.0")

        # Sub case: Sell order has worst price (higher) than full book's worst -> no impact on order book

        event = {"type": "open", "order_id": "5", "remaining_size": "5.555", "price": "5.00", "side": "sell"}
        target.handle_event(event)

        expected_best_ask_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("1.0"), {"2", "4"})}
        expected_best_bid_levels = {}
        expected_ask_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        expected_bid_ids = {}
        expected_worst_ask_price = D("2.00")
        expected_worst_bid_price = D("-1.0")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

        # Sub case: Sell order has better price (lower) than full book's worst -> insert and replace

        event = {"type": "open", "order_id": "6", "remaining_size": "6.00", "price": "0.60", "side": "sell"}
        target.handle_event(event)

        expected_best_ask_levels = {"0.60": (D("6.00"), {"6"}), "1.00": (D("2.01"), {"1", "3"})}
        expected_best_bid_levels = {}
        expected_ask_ids = {"1": ("1.00", D("1.005")), "3": ("1.00", D("1.005")), "6": ("0.60", D("6.00"))}
        expected_bid_ids = {}
        expected_worst_ask_price = D("1.00")
        expected_worst_bid_price = D("-1.0")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_open_buy_new_price_levels(self):
        target = ob.OrderBook(max_levels=2)

        # Sub case: Completely empty order book

        event = {"type": "open", "order_id": "1", "remaining_size": "1.005", "price": "1.00", "side": "buy"}

        target.handle_event(event)

        expected_best_ask_levels = {}
        expected_best_bid_levels = {"1.00": (D("1.005"), {"1"})}
        expected_ask_ids = {}
        expected_bid_ids = {"1": ("1.00", D("1.005"))}
        expected_worst_ask_price = D("-1.0")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

        # Sub case: Non-empty, non-full order book

        event = {"type": "open", "order_id": "2", "remaining_size": "0.5", "price": "2.00", "side": "buy"}

        target.handle_event(event)

        expected_best_ask_levels = {}
        expected_best_bid_levels = {"1.00": (D("1.005"), {"1"}), "2.00": (D("0.5"), {"2"})}
        expected_ask_ids = {}
        expected_bid_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5"))}
        expected_worst_ask_price = D("-1.0")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_open_buy_same_price_levels(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {}
        target.best_bid_levels = {"1.00": (D("1.005"), {"1"}), "2.00": (D("0.5"), {"2"})}
        target.ask_ids = {}
        target.bid_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5"))}
        target.worst_ask_price = D("-1.0")
        target.worst_bid_price = D("1.00")

        # Open orders come in for existing price levels -> quantity should increase accordingly

        event = {"type": "open", "order_id": "3", "remaining_size": "1.005", "price": "1.00", "side": "buy"}
        target.handle_event(event)
        event = {"type": "open", "order_id": "4", "remaining_size": "0.5", "price": "2.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {}
        expected_best_bid_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("1.0"), {"2", "4"})}
        expected_ask_ids = {}
        expected_bid_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        expected_worst_ask_price = D("-1.0")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_open_buy_full_book(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {}
        target.best_bid_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("1.0"), {"2", "4"})}
        target.ask_ids = {}
        target.bid_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        target.worst_ask_price = D("-1.0")
        target.worst_bid_price = D("1.00")

        # Sub case: Buy order has worst price (lower) than full book's worst -> no impact on order book

        event = {"type": "open", "order_id": "5", "remaining_size": "5.555", "price": "0.50", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {}
        expected_best_bid_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("1.0"), {"2", "4"})}
        expected_ask_ids = {}
        expected_bid_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        expected_worst_ask_price = D("-1.0")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

        # Sub case: Buy order has better price (higher) than full book's worst -> insert and replace

        event = {"type": "open", "order_id": "6", "remaining_size": "6.00", "price": "6.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {}
        expected_best_bid_levels = {"2.00": (D("1.0"), {"2", "4"}), "6.00": (D("6.00"), {"6"})}
        expected_ask_ids = {}
        expected_bid_ids = {"2": ("2.00", D("0.5")), "4": ("2.00", D("0.5")), "6": ("6.00", D("6.00"))}
        expected_worst_ask_price = D("-1.0")
        expected_worst_bid_price = D("2.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    ###################
    # Done Unit Tests #
    ###################

    def test_done_order_exists_in_book_and_not_only_order_in_level(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {"10.00": (D("10.00"), {"5", "6"})}
        target.best_bid_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("1.0"), {"2", "4"})}
        target.ask_ids = {"5": ("10.00", D("3.5")), "6": ("10.00", D("6.5"))}
        target.bid_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        target.worst_ask_price = D("10.00")
        target.worst_bid_price = D("1.00")

        # Sub case: Done sell order -> Impacts ask values

        event = {"type": "done", "order_id": "6", "remaining_size": "0", "price": "10.00", "side": "sell"}
        target.handle_event(event)

        expected_best_ask_levels = {"10.00": (D("3.5"), {"5"})}
        expected_best_bid_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("1.0"), {"2", "4"})}
        expected_ask_ids = {"5": ("10.00", D("3.5"))}
        expected_bid_ids = {"1": ("1.00", D("1.005")), "2": ("2.00", D("0.5")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        expected_worst_ask_price = D("10.00")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

        # Sub case: Done buy order -> Impacts bid values

        event = {"type": "done", "order_id": "2", "remaining_size": "0.1", "price": "2.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {"10.00": (D("3.5"), {"5"})}
        expected_best_bid_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("0.5"), {"4"})}
        expected_ask_ids = {"5": ("10.00", D("3.5"))}
        expected_bid_ids = {"1": ("1.00", D("1.005")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        expected_worst_ask_price = D("10.00")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_done_order_exists_in_book_and_only_order_in_level(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {"10.00": (D("3.5"), {"5"})}
        target.best_bid_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("0.5"), {"4"})}
        target.ask_ids = {"5": ("10.00", D("3.5"))}
        target.bid_ids = {"1": ("1.00", D("1.005")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        target.worst_ask_price = D("10.00")
        target.worst_bid_price = D("1.00")

        # Sub case: Done sell order -> Impacts ask values

        event = {"type": "done", "order_id": "5", "remaining_size": "3.5", "price": "10.00", "side": "sell"}
        target.handle_event(event)

        expected_best_ask_levels = {}
        expected_best_bid_levels = {"1.00": (D("2.01"), {"1", "3"}), "2.00": (D("0.5"), {"4"})}
        expected_ask_ids = {}
        expected_bid_ids = {"1": ("1.00", D("1.005")), "3": ("1.00", D("1.005")), "4": ("2.00", D("0.5"))}
        expected_worst_ask_price = D("-1.0")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

        # Sub case: Done buy order -> Impacts bid values

        event = {"type": "done", "order_id": "4", "remaining_size": "0", "price": "2.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {}
        expected_best_bid_levels = {"1.00": (D("2.01"), {"1", "3"})}
        expected_ask_ids = {}
        expected_bid_ids = {"1": ("1.00", D("1.005")), "3": ("1.00", D("1.005"))}
        expected_worst_ask_price = D("-1.0")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_done_order_exists_in_book_and_only_order_in_level_updates_worst_price(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {"5.00": (D("1.0"), {"5"}), "6.00": (D("1.0"), {"6"})}
        target.best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("1.0"), {"2"})}
        target.ask_ids = {"5": ("5.00", D("1.0")), "6": ("6.00", D("1.0"))}
        target.bid_ids = {"1": ("1.00", D("1.0")), "2": ("2.00", D("1.0"))}
        target.worst_ask_price = D("6.00")
        target.worst_bid_price = D("1.00")

        # Sub case: Done sell order -> Impacts ask values

        event = {"type": "done", "order_id": "6", "remaining_size": "0", "price": "6.00", "side": "sell"}
        target.handle_event(event)

        expected_best_ask_levels = {"5.00": (D("1.0"), {"5"})}
        expected_best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("1.0"), {"2"})}
        expected_ask_ids = {"5": ("5.00", D("1.0"))}
        expected_bid_ids = {"1": ("1.00", D("1.0")), "2": ("2.00", D("1.0"))}
        expected_worst_ask_price = D("5.00")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

        # Sub case: Done buy order -> Impacts bid values

        event = {"type": "done", "order_id": "1", "remaining_size": "0", "price": "1.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {"5.00": (D("1.0"), {"5"})}
        expected_best_bid_levels = {"2.00": (D("1.0"), {"2"})}
        expected_ask_ids = {"5": ("5.00", D("1.0"))}
        expected_bid_ids = {"2": ("2.00", D("1.0"))}
        expected_worst_ask_price = D("5.00")
        expected_worst_bid_price = D("2.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_done_empty_book(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {}
        target.best_bid_levels = {}
        target.ask_ids = {}
        target.bid_ids = {}
        target.worst_ask_price = D("-1.0")
        target.worst_bid_price = D("-1.0")

        # Done orders on empty book -> no effect regardless of side

        event = {"type": "done", "order_id": "a", "remaining_size": "0", "price": "1.11", "side": "sell"}
        target.handle_event(event)
        event = {"type": "done", "order_id": "b", "remaining_size": "6.00", "price": "6.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {}
        expected_best_bid_levels = {}
        expected_ask_ids = {}
        expected_bid_ids = {}
        expected_worst_ask_price = D("-1.0")
        expected_worst_bid_price = D("-1.0")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_done_order_does_not_exist_in_book(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {"5.00": (D("1.0"), {"5"}), "6.00": (D("1.0"), {"6"})}
        target.best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("1.0"), {"2"})}
        target.ask_ids = {"5": ("5.00", D("1.0")), "6": ("6.00", D("1.0"))}
        target.bid_ids = {"1": ("1.00", D("1.0")), "2": ("2.00", D("1.0"))}
        target.worst_ask_price = D("6.00")
        target.worst_bid_price = D("1.00")

        event = {"type": "done", "order_id": "3", "remaining_size": "0", "price": "6.00", "side": "sell"}
        target.handle_event(event)
        event = {"type": "done", "order_id": "4", "remaining_size": "0", "price": "1.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {"5.00": (D("1.0"), {"5"}), "6.00": (D("1.0"), {"6"})}
        expected_best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("1.0"), {"2"})}
        expected_ask_ids = {"5": ("5.00", D("1.0")), "6": ("6.00", D("1.0"))}
        expected_bid_ids = {"1": ("1.00", D("1.0")), "2": ("2.00", D("1.0"))}
        expected_worst_ask_price = D("6.00")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    ####################
    # Match Unit Tests #
    ####################

    def test_match_empty_book(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {}
        target.best_bid_levels = {}
        target.ask_ids = {}
        target.bid_ids = {}
        target.worst_ask_price = D("-1.0")
        target.worst_bid_price = D("-1.0")

        # Match orders on empty book -> no effect regardless of side

        event = {"type": "match", "maker_order_id": "a", "size": "0", "price": "1.11", "side": "sell"}
        target.handle_event(event)
        event = {"type": "match", "maker_order_id": "b", "size": "6.00", "price": "6.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {}
        expected_best_bid_levels = {}
        expected_ask_ids = {}
        expected_bid_ids = {}
        expected_worst_ask_price = D("-1.0")
        expected_worst_bid_price = D("-1.0")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_match_complete_fill(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {"5.00": (D("1.0"), {"5"}), "6.00": (D("1.0"), {"6"})}
        target.best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("1.0"), {"2"})}
        target.ask_ids = {"5": ("5.00", D("1.0")), "6": ("6.00", D("1.0"))}
        target.bid_ids = {"1": ("1.00", D("1.0")), "2": ("2.00", D("1.0"))}
        target.worst_ask_price = D("6.00")
        target.worst_bid_price = D("1.00")

        # Sub case: Match sell order -> Impacts ask values

        event = {"type": "match", "maker_order_id": "6", "size": "1.0", "price": "6.00", "side": "sell"}
        target.handle_event(event)

        expected_best_ask_levels = {"5.00": (D("1.0"), {"5"})}
        expected_best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("1.0"), {"2"})}
        expected_ask_ids = {"5": ("5.00", D("1.0"))}
        expected_bid_ids = {"1": ("1.00", D("1.0")), "2": ("2.00", D("1.0"))}
        expected_worst_ask_price = D("5.00")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

        # Sub case: Match buy order -> Impacts bid values

        event = {"type": "match", "maker_order_id": "1", "size": "1.0", "price": "1.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {"5.00": (D("1.0"), {"5"})}
        expected_best_bid_levels = {"2.00": (D("1.0"), {"2"})}
        expected_ask_ids = {"5": ("5.00", D("1.0"))}
        expected_bid_ids = {"2": ("2.00", D("1.0"))}
        expected_worst_ask_price = D("5.00")
        expected_worst_bid_price = D("2.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    def test_match_partial_fill(self):
        target = ob.OrderBook(max_levels=2)
        target.best_ask_levels = {"5.00": (D("1.0"), {"5"}), "6.00": (D("1.0"), {"6"})}
        target.best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("1.0"), {"2"})}
        target.ask_ids = {"5": ("5.00", D("1.0")), "6": ("6.00", D("1.0"))}
        target.bid_ids = {"1": ("1.00", D("1.0")), "2": ("2.00", D("1.0"))}
        target.worst_ask_price = D("6.00")
        target.worst_bid_price = D("1.00")

        # Sub case: Match sell order -> Impacts ask values

        event = {"type": "match", "maker_order_id": "6", "size": "0.7", "price": "6.00", "side": "sell"}
        target.handle_event(event)

        expected_best_ask_levels = {"5.00": (D("1.0"), {"5"}), "6.00": (D("0.3"), {"6"})}
        expected_best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("1.0"), {"2"})}
        expected_ask_ids = {"5": ("5.00", D("1.0")), "6": ("6.00", D("0.3"))}
        expected_bid_ids = {"1": ("1.00", D("1.0")), "2": ("2.00", D("1.0"))}
        expected_worst_ask_price = D("6.00")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

        # Sub case: Match buy order -> Impacts bid values

        event = {"type": "match", "maker_order_id": "2", "size": "0.45", "price": "1.00", "side": "buy"}
        target.handle_event(event)

        expected_best_ask_levels = {"5.00": (D("1.0"), {"5"}), "6.00": (D("0.3"), {"6"})}
        expected_best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("0.55"), {"2"})}
        expected_ask_ids = {"5": ("5.00", D("1.0")), "6": ("6.00", D("0.3"))}
        expected_bid_ids = {"1": ("1.00", D("1.0")), "2": ("2.00", D("0.55"))}
        expected_worst_ask_price = D("6.00")
        expected_worst_bid_price = D("1.00")

        self._assert_order_book_values(target, expected_best_ask_levels, expected_best_bid_levels, expected_ask_ids,
                                       expected_bid_ids, expected_worst_ask_price, expected_worst_bid_price)

    ######################
    # Unit Tests Helpers #
    ######################

    def _assert_order_book_values(self,
                                  order_book,
                                  expected_best_ask_levels,
                                  expected_best_bid_levels,
                                  expected_ask_ids,
                                  expected_bid_ids,
                                  expected_worst_ask_price,
                                  expected_worst_bid_price):

        actual_best_ask_levels = order_book.best_ask_levels
        actual_best_bid_levels = order_book.best_bid_levels
        actual_ask_ids = order_book.ask_ids
        actual_bid_ids = order_book.bid_ids
        actual_worst_ask_price = order_book.worst_ask_price
        actual_worst_bid_price = order_book.worst_bid_price

        self.assertEqual(len(expected_best_ask_levels), len(actual_best_ask_levels))
        for expected_ask_price in expected_best_ask_levels.keys():
            self.assertTrue(expected_ask_price in actual_best_ask_levels)

            (expected_quantity, expected_ask_order_ids) = expected_best_ask_levels[expected_ask_price]
            (actual_quantity, actual_ask_order_ids) = actual_best_ask_levels[expected_ask_price]

            self.assertEqual(expected_quantity, actual_quantity)
            self.assertEqual(len(expected_ask_order_ids), len(actual_ask_order_ids))
            for order_id in expected_ask_order_ids:
                self.assertTrue(order_id in actual_ask_order_ids)

        self.assertEqual(len(expected_best_bid_levels), len(actual_best_bid_levels))
        for expected_bid_price in expected_best_bid_levels.keys():
            self.assertTrue(expected_bid_price in actual_best_bid_levels)

            (expected_quantity, expected_bid_order_ids) = expected_best_bid_levels[expected_bid_price]
            (actual_quantity, actual_bid_order_ids) = actual_best_bid_levels[expected_bid_price]

            self.assertEqual(expected_quantity, actual_quantity)
            self.assertEqual(len(expected_bid_order_ids), len(actual_bid_order_ids))
            for order_id in expected_bid_order_ids:
                self.assertTrue(order_id in actual_bid_order_ids)

        self.assertEqual(len(expected_ask_ids), len(actual_ask_ids))
        for expected_ask_id in expected_ask_ids.keys():
            self.assertTrue(expected_ask_id in actual_ask_ids)

            expected_price = expected_ask_ids[expected_ask_id][0]
            expected_quantity = expected_ask_ids[expected_ask_id][1]
            actual_price = actual_ask_ids[expected_ask_id][0]
            actual_quantity = actual_ask_ids[expected_ask_id][1]

            self.assertEqual(expected_price, actual_price)
            self.assertEqual(expected_quantity, actual_quantity)

        self.assertEqual(len(expected_bid_ids), len(actual_bid_ids))
        for expected_bid_id in expected_bid_ids.keys():
            self.assertTrue(expected_bid_id in actual_bid_ids)

            expected_price = expected_bid_ids[expected_bid_id][0]
            expected_quantity = expected_bid_ids[expected_bid_id][1]
            actual_price = actual_bid_ids[expected_bid_id][0]
            actual_quantity = actual_bid_ids[expected_bid_id][1]

            self.assertEqual(expected_price, actual_price)
            self.assertEqual(expected_quantity, actual_quantity)

        self.assertEqual(expected_worst_ask_price, actual_worst_ask_price)
        self.assertEqual(expected_worst_bid_price, actual_worst_bid_price)


if __name__ == '__main__':
    unittest.main()
