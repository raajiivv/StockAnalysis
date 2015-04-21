'''
Created on Apr 16, 2015

@author: rajiv
'''
from bottle import Bottle, run, route, response, hook
import tweetRepository as tr

myApp = Bottle()

@hook('after_request')
def enable_cors():
    """
    You need to add some headers to each request.
    Don't use the wildcard '*' for Access-Control-Allow-Origin in production.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'


@route('/getTopTweets/<sym>')
def getTopTweetsbySymbol(sym):
    topTweets = tr.get_top_tweets(sym)
    response.content_type = 'application/json'
    return topTweets

@route('/getAllTweets/<sym>')
def getAllTweetsbySymbol(sym):
    tweets = tr.get_all_tweets(sym)
    response.content_type = 'application/json'
    return tweets

@route('/getSentimentCount/<sym>')
def getSentimentCountbySymbol(sym):
    count = tr.get_sentiment_count(sym)
    response.content_type = 'application/json'
    return count

@route('/getSentimentChange/<sym>')
def getSentimentChangebySymbol(sym):
    print True
    sentiment = tr.get_sentiment_change(sym)
    response.content_type = 'application/json'
    return sentiment

@route('/getSentiments')
def getSentiments():
    sentiments = tr.get_sentiments()
    response.content_type = 'application/json'
    return sentiments

@route('/getPredictedStock/<sym>')
def getPredictedStock(sym):
    predicted_stock = tr.get_predicted_stock(sym)
    response.content_type = 'application/json'
    return predicted_stock

run(host = "0.0.0.0", port = 8800, interval = 1, reloader = True, debug = True)

