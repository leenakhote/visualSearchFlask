from flask import Blueprint
main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request
from flask.json import jsonify

#@main.route("/<int:user_id>/visualsearch", methods=["GET"])
@main.route("/visualsearch", methods=["GET"])
def getVisualSearch():
    imgUrl = request.args.get('imgUrl')
    print("get visual search url call")
    result = recommendation_engine.getVisualSearch(imgUrl)
    #print "****************", result
    return jsonify(result)

def create_app(spark_context, dataset_path):
    global recommendation_engine
    recommendation_engine = RecommendationEngine(spark_context, dataset_path)
    app = Flask(__name__)
    app.register_blueprint(main)
    return app
