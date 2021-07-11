import unittest
from market_data_feed import order_book as ob, market_data_feed_client as mdf
from decimal import Decimal as D


class TestMarketDataFeedClient(unittest.TestCase):

    def test_get_inside_levels_printout_happy_path(self):
        test_order_book = ob.OrderBook(book_size=3)
        test_order_book.best_ask_levels = {"5.00": (D("5.0"), {"5"}), "6.00": (D("6.0"), {"6"}), "7.00": (D("7.0"), {"7"})}
        test_order_book.best_bid_levels = {"1.00": (D("1.0"), {"1"}), "2.00": (D("2.0"), {"2"}), "3.00": (D("3.0"), {"3"})}

        target = mdf.MarketDataFeedClient()
        target.order_book = test_order_book

        expected = (" 6.00000 @ 6.00\n"
                    " 5.00000 @ 5.00\n"
                    "----------------\n"
                    " 3.00000 @ 3.00\n"
                    " 2.00000 @ 2.00")

        actual = target.get_inside_levels_printout(2)

        self._assertEqualLineByLine(expected, actual)

    def test_get_inside_levels_printout_partial_book(self):
        test_order_book = ob.OrderBook(book_size=3)
        test_order_book.best_ask_levels = {"5.00": (D("5.0"), {"5"})}
        test_order_book.best_bid_levels = {"1.00": (D("1.0"), {"1"})}

        target = mdf.MarketDataFeedClient()
        target.order_book = test_order_book

        expected = (" 5.00000 @ 5.00\n"
                    "----------------\n"
                    " 1.00000 @ 1.00")

        actual = target.get_inside_levels_printout(2)

        self._assertEqualLineByLine(expected, actual)

    def _assertEqualLineByLine(self, expected, actual):
        expected_lines = expected.split("\n")
        actual_lines = actual.split("\n")

        self.assertEqual(len(expected_lines), len(actual_lines))
        for i in range(len(expected_lines)):
            self.assertEqual(expected_lines[i], actual_lines[i])


if __name__ == '__main__':
    unittest.main()
