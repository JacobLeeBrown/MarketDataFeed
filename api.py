
import datetime
from flask import Flask
from flask_cors import CORS
from flask_restful import Resource, Api, reqparse
from market_data_feed.market_data_feed_client import MarketDataFeedClient

app = Flask(__name__)
CORS(app)
api = Api(app)

mdf_client = MarketDataFeedClient()


class MarketDataFeed(Resource):

    def get(self):

        parser = reqparse.RequestParser()
        parser.add_argument('action', type=str)
        args = parser.parse_args()
        action = args["action"]

        if 'start' == action:
            mdf_client.start()
            return {"msg": "Market Data Feed Started"}
        elif "stop" == action:
            mdf_client.close()
            return {"msg": "Market Data Feed Stopped"}
        elif "levels" == action:
            level_count = 5
            msg_str = "Inside BTC-USD Levels as of: \n{}\n\n{}".format(
                str(datetime.datetime.now()),
                mdf_client.get_inside_levels_printout(level_count))
            return {"msg": msg_str}

        return {"msg": format("Unknown action parameter: %s", action)}


api.add_resource(MarketDataFeed, "/feed")


if __name__ == "__main__":
    app.run(debug=True)
