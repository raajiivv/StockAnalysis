'''
Created on Apr 16, 2015

@author: rajiv
'''
from bottle import Bottle, run, route, response, hook
import tweetRepository

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


@route('/getTopTweetsbySymbol/<sym>')
def getTopTweetsbySymbol(sym):
    sym = sym
    print sym
    topTweets = tweetRepository.getTopTweets(sym)
    response.content_type = 'application/json'
    return topTweets

run(host = "0.0.0.0", port = 8800, interval = 1, reloader = True, debug = True)

