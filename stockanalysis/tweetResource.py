'''
Created on Apr 16, 2015

@author: rajiv
'''
from bottle import run, route, response
import tweetRepository

mySymbols = {"$AAPL"}

@route('/getTopTweetsbySymbol/<sym>')
def getTopTweetsbySymbol(sym):
    sym = sym
    print sym
    topTweets = tweetRepository.getTopTweets(sym)
    response.content_type = 'application/json'
    return topTweets

run(host = "0.0.0.0", port = 8800, interval = 1, reloader = True, debug = True)

