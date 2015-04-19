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
    db_tweets = db.tweets
    mean_results_by_symbol = list(db_tweets.aggregate([{"$match" : { "symbol" : symbol }},{"$group" : {"_id": "$sentiment", "avgScore" : { "$avg" : "$score"}}}]))
    #print mean_results_by_symbol
    sentiment['symbol'] = symbol
    #sentiment['avg_pos_score'] = mean_results_by_symbol[1]['avgScore'] 
    #sentiment['avg_neg_score'] = mean_results_by_symbol[2]['avgScore']
    sentiment['net_sentiment'] = mean_results_by_symbol[1]['avgScore'] + mean_results_by_symbol[2]['avgScore']
    sentiment['time'] = datetime.datetime.utcnow().strftime("%a %b %d %X +0000 %Y")
    sentiment['stock_value'] = stock_value
    db.sentiments.remove({"symbol":symbol})
    db.sentiments.insert(sentiment)

def store_top_tweets(symbol, db):
    db_tweets = db.tweets
    most_positive_tweet = db_tweets.find_one({"$and": [{"symbol": symbol},{"sentiment" : "positive"}]}, {"_id":0, "id":0, "score":0}, sort=[("score", -1)])
    most_negative_tweet = db_tweets.find_one({"$and": [{"symbol": symbol},{"sentiment" : "negative"}]}, {"_id":0, "id":0, "score":0}, sort=[("score", 1)])
    db.top_tweets.remove({"symbol":symbol})
    db.top_tweets.insert(most_negative_tweet)
    db.top_tweets.insert(most_positive_tweet)
    

