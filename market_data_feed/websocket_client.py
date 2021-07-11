# market_data_feed/websocket_client.py
# original author: Daniel Paquin
#
#
# Template object to receive messages from the Coinbase WebSocket Feed. Refactored from original slightly to remove
# features not needed for this project as well as some minor cleanup.

import json
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException


class WebSocketClient(object):
    def __init__(
            self,
            url="wss://ws-feed.pro.coinbase.com",
            products=None,
            message_type="subscribe",
            message_queue=None,
            should_print=True,
            channels=None):
        self.url = url
        self.products = products
        self.channels = channels
        self.type = message_type
        self.stop = True
        self.error = None
        self.ws = None
        self.thread = None
        self.keepAlive = None
        self.should_print = should_print
        self.message_queue = message_queue

    def start(self):
        def _go():
            self._connect()
            self._listen()
            self._disconnect()

        self.stop = False
        self.on_open()
        self.thread = Thread(target=_go)
        self.keepAlive = Thread(target=self._keep_alive)
        self.thread.start()

    def _connect(self):
        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]

        if self.url[-1] == "/":
            self.url = self.url[:-1]

        if self.channels is None:
            self.channels = [{"name": "ticker", "product_ids": [product_id for product_id in self.products]}]
            sub_params = {"type": "subscribe", "product_ids": self.products, "channels": self.channels}
        else:
            sub_params = {"type": "subscribe", "product_ids": self.products, "channels": self.channels}

        self.ws = create_connection(self.url)

        self.ws.send(json.dumps(sub_params))

    def _keep_alive(self, interval=30):
        while self.ws.connected:
            self.ws.ping("keepalive")
            time.sleep(interval)

    def _listen(self):
        self.keepAlive.start()
        while not self.stop:
            try:
                data = self.ws.recv()
                msg = json.loads(data)
            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass
        finally:
            self.keepAlive.join()

        self.on_close()

    def close(self):
        self.stop = True    # will only disconnect after next msg recv
        self._disconnect()  # force disconnect so threads can join
        self.thread.join()

    def on_open(self):
        if self.should_print:
            print("-- Subscribed! --\n")

    def on_close(self):
        if self.should_print:
            print("\n-- Socket Closed --")

    def on_message(self, msg):
        if self.should_print:
            print(msg)
        if self.message_queue:
            self.message_queue.append(msg)

    def on_error(self, e, data=None):
        self.error = e
        self.stop = True
        print("{} - data: {}".format(e, data))
