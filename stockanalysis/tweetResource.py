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

