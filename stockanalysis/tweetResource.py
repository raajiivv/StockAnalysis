'''
Created on Apr 16, 2015

@author: rajiv
'''
from bottle import run, route
import json
import tweetRepository

mySymbols = {"$AAPL"}

route("/getTopTweetsbySymbol/<sym>")
def getTopTweetsbySymbol(sym):
    topTweets = tweetRepository.getTopTweets(sym)
    return topTweets

run(host = "0.0.0.0", port = 8800, interval = 1, reloader = True, debug = True)

