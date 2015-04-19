'''
Created on Apr 18, 2015

@author: rajiv
'''

import datetime
import sys

import pymongo


def store_tweets(tweets , db):
    db_tweets = db.tweets

    for tweet in tweets:
        db_id = db_tweets.insert(tweet)
    db_count = db_tweets.count()
    print "Tweets stored in MongoDB! Number of documents in twitter_db: %d" % db_count
    return

def delete_tweets(db):
    db.tweets.drop()
    #print "Data from twitter_db instance has been deleted!"
    return

def store_sentiment(symbol, db, stock_value):
    sentiment = {}
    tweets = db.tweets
    mean_results_by_symbol = list(tweets.aggregate([{"$match" : { "symbol" : symbol }},{"$group" : {"_id": "$sentiment", "avgScore" : { "$avg" : "$score"}}}]))
    #print mean_results_by_symbol
    sentiment['symbol'] = symbol
    #sentiment['avg_pos_score'] = mean_results_by_symbol[1]['avgScore'] 
    #sentiment['avg_neg_score'] = mean_results_by_symbol[2]['avgScore']
    sentiment['net_sentiment'] = mean_results_by_symbol[1]['avgScore'] + mean_results_by_symbol[2]['avgScore']
    sentiment['time'] = datetime.datetime.utcnow().strftime("%a %b %d %X +0000 %Y")
    sentiment['stock_value'] = stock_value
    print sentiment
    print
    db.sentiments.insert(sentiment)






