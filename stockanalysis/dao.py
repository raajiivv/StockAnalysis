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
    net_sentiment = 0
    for n in mean_results_by_symbol:
        net_sentiment = net_sentiment+n['avgScore']
    sentiment['net_sentiment'] = net_sentiment
    sentiment['time'] = datetime.datetime.utcnow().strftime("%a %b %d %X +0000 %Y")
    sentiment['stock_value'] = stock_value
    db.sentiments.remove({"symbol":symbol})
    print symbol, " : ", net_sentiment
    db.sentiments.insert(sentiment)

def store_top_tweets(symbol, db):
    db_tweets = db.tweets
    most_positive_tweet = db_tweets.find_one({"$and": [{"symbol": symbol},{"sentiment" : "positive"}]}, {"_id":0, "id":0, "score":0}, sort=[("score", -1)])
    most_negative_tweet = db_tweets.find_one({"$and": [{"symbol": symbol},{"sentiment" : "negative"}]}, {"_id":0, "id":0, "score":0}, sort=[("score", 1)])
    db.top_tweets.remove({"symbol":symbol})
    db.top_tweets.insert(most_negative_tweet)
    db.top_tweets.insert(most_positive_tweet)
    
def get_top_tweets(symbol,db):
    db_top_tweets = db.top_tweets
    tweets = db_top_tweets.find({"symbol": symbol}, {"_id":0 })
    return tweets

def get_tweets(symbol, db):
    db_tweets = db.tweets
    tweets = db_tweets.find({"symbol": symbol}, {"_id":0, "id":0, "score":0})
    return tweets

def get_sentiment_count(symbol, db):
    db_tweets = db.tweets
    count = []
    count.append(db_tweets.find({"$and": [{"symbol": symbol},{"sentiment" : "positive"}]}).count())
    count.append(db_tweets.find({"$and": [{"symbol": symbol},{"sentiment" : "negative"}]}).count())
    count.append(db_tweets.find({"$and": [{"symbol": symbol},{"sentiment" : "neutral"}]}).count())
    return count
    
    

