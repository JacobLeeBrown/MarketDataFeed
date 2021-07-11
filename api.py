# api.py
# original author: Jacob Brown
#
#
# Main module for this project. RESTful API for interfacing with underlying market feed client.

import logging
import datetime
from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
from market_data_feed.market_data_feed_client import MarketDataFeedClient
from market_data_feed import time_util

logging.basicConfig(
    format='%(asctime)s - %(name)10s - %(levelname)7s - %(message)s', level=logging.DEBUG
)

app = Flask(__name__)
CORS(app)
api = Api(app)

mdf_client = MarketDataFeedClient()


class MarketDataFeedAPI(Resource):

    def get(self):
        start_time = time_util.current_milli_time()
        logging.info("Received MarketDataFeedAPI GET Request")

        parser = reqparse.RequestParser()
        parser.add_argument("action", type=str)
        args = parser.parse_args()
        action = args["action"]

        if "start" == action:
            msg_str = self._start()
        elif "stop" == action:
            msg_str = self._stop()
        elif "levels" == action:
            msg_str = self._levels()
        else:
            msg_str = "Action ({}) not recognized".format(action)
            logging.warning(msg_str)

        logging.info("End MarketDataFeedAPI GET Request. time_taken={} ms".format(
            time_util.current_milli_time() - start_time))
        return {"msg": msg_str}

    def _start(self):
        mdf_client.start()
        return "Market Data Feed Started"

    def _stop(self):
        mdf_client.close()
        return "Market Data Feed Stopped"

    def _levels(self):
        level_count = 5
        msg_str = "Inside BTC-USD Levels as of: \n{}\n\n{}".format(
            str(datetime.datetime.now()),
            mdf_client.get_inside_levels_printout(level_count))
        return msg_str


api.add_resource(MarketDataFeedAPI, "/feed")


if __name__ == "__main__":
    app.run(debug=True)
