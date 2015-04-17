'''
Created on Apr 16, 2015

@author: rajiv
'''
import json

def getTopTweets(sym):
    topTweets = []
    topPositive = {"symbol": "$AAPL", "timestamp": "Fri Apr 17 04:45:15 +0000 2015", "text": "$OSLH Has Become One Of The Most Powerful Penny Stocks This Week! Special Update: http://t.co/rQZs5De8kI $TRX $AAPL $GOOG", "screen_name":"StockDawgzz"}
    topNegative = {"symbol": "$AAPL", "timestamp": "Fri Apr 17 02:39:53 +0000 2015", "text": "Analysts Harsh On SanDisks Q1 Suffering $AAPL $SNDK http://t.co/PFErzmdzen", "screen_name":"leegarcia121"}
    topTweets.append(topPositive)
    topTweets.append(topNegative)
    topTweets = json.dumps(topTweets, ensure_ascii = False)
    print topTweets
    return topTweets

getTopTweets("AAPL")