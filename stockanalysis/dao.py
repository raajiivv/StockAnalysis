'''
Created on Apr 18, 2015

@author: rajiv
'''

import datetime

# Stores the enriched tweets
def store_tweets(tweets , db):
    db_tweets = db.tweets

    for tweet in tweets:
        db_id = db_tweets.insert(tweet)
    db_count = db_tweets.count()
    print "Tweets stored in MongoDB! Number of documents in twitter_db: %d" % db_count
    return

# Deletes the tweets data
def delete_tweets(db):
    db.tweets.drop()
    #print "Data from twitter_db instance has been deleted!"
    return

# Deletes the sentiment count
def delete_sentiment_count(db):
    db.sentiment_count.drop()
    return

# Stores sentiment and prediction values
def store_sentiment(symbol, db, stock_value):
    sentiment = {}
    prediction_values = {}
    net_sentiment = 0
    sentiment_change = 0
    stock_change = 0
    delta_ratio = 0
    db_tweets = db.tweets
    db_sentiments = db.sentiments
    db_prediction = db.prediction
    mean_results_by_symbol = list(db_tweets.aggregate([{"$match" : { "symbol" : symbol }},{"$group" : {"_id": "$sentiment", "avgScore" : { "$avg" : "$score"}}}]))
    #print mean_results_by_symbol
    for n in mean_results_by_symbol:
        net_sentiment = net_sentiment+n['avgScore']
    
    prev_sentiment = db_sentiments.find_one({"symbol" : symbol})
    print prev_sentiment
    # delta values to track changes in sentiments and stock values
    if prev_sentiment != None:
        sentiment_change = prev_sentiment['net_sentiment'] - net_sentiment
        stock_change = float(prev_sentiment['stock_value']) - float(stock_value) 
        if sentiment_change == 0: 
            delta_ratio = 0
        else:
            delta_ratio = stock_change/sentiment_change
        
    prediction =  db_prediction.find_one({"symbol":symbol})
    
    # Multiplication factor to predict the changes in stock value
    if prediction !=None :
        count_delta = prediction['count_delta']
        multiplication_factor = db_prediction.find_one({"symbol":symbol})['multiplication_factor'] 
        multiplication_factor = ((count_delta*multiplication_factor) + (delta_ratio))/(count_delta+1)
    else:
        count_delta = 0
        multiplication_factor = delta_ratio
    count_delta = count_delta + 1
    # Updates sentiment and stock values
    sentiment['symbol'] = symbol
    sentiment['net_sentiment'] = net_sentiment
    sentiment['time'] = datetime.datetime.utcnow().strftime("%a %b %d %X +0000 %Y")
    sentiment['stock_value'] = stock_value

    # Updates delta values and multiplication factor
    prediction_values['symbol'] = symbol
    prediction_values['sentiment_change'] = sentiment_change
    prediction_values['stock_change'] = stock_change
    prediction_values['count_delta'] = count_delta
    prediction_values['multiplication_factor'] = multiplication_factor
    
    db_sentiments.remove({"symbol":symbol})
    db_prediction.remove({"symbol":symbol})
    print symbol, " : ", net_sentiment
    db_sentiments.insert(sentiment)
    db_prediction.insert(prediction_values)
    return
    
# Stores top positive and negative tweets for easy access
def store_top_tweets(symbol, db):
    db_tweets = db.tweets
    most_positive_tweets = db_tweets.find({"$and": [{"symbol": symbol},{"sentiment" : "positive"}]}, {"_id":0, "id":0, "score":0}, sort=[("score", -1)], limit = 10)
    most_negative_tweets = db_tweets.find({"$and": [{"symbol": symbol},{"sentiment" : "negative"}]}, {"_id":0, "id":0, "score":0}, sort=[("score", 1)], limit = 10)
    if most_negative_tweets !=None and most_positive_tweets!=None:    
        db.top_tweets.remove({"symbol":symbol})
        db.top_tweets.insert(most_negative_tweets)
        db.top_tweets.insert(most_positive_tweets)
        return
        
# Stores the percentage counts of positive, negative and neutral sentiments
def store_sentiment_count(symbol, db):
    db_tweets = db.tweets
    db_sentiment_count = db.sentiment_count
    count = []
    sentiment_count = {}
    sentiment_count['symbol'] = symbol
    sentiment_count['time'] = datetime.datetime.utcnow().strftime("%a %b %d %X +0000 %Y")
    count.append(db_tweets.find({"$and": [{"symbol": symbol},{"sentiment" : "positive"}]}).count())
    count.append(db_tweets.find({"$and": [{"symbol": symbol},{"sentiment" : "negative"}]}).count())
    count.append(db_tweets.find({"$and": [{"symbol": symbol},{"sentiment" : "neutral"}]}).count())
    total_count = count[0]+count[1]+count[2]
    sentiment_count['positvie'] =  ("%.2f" %(count[0]*100.0/total_count)) if total_count>0 else 0
    sentiment_count['negativie'] = ("%.2f" %(count[1]*100.0/total_count)) if total_count>0 else 0
    sentiment_count['neutral'] = ("%.2f" %(count[2]*100.0/total_count)) if total_count>0 else 0
    db_sentiment_count.insert(sentiment_count)
    return

# Retrieves top positive and negative tweets    
def get_top_tweets(symbol,db):
    db_top_tweets = db.top_tweets
    tweets = db_top_tweets.find({"symbol": symbol}, {"_id":0 })
    return tweets

# Retrieves all tweets
def get_tweets(symbol, db):
    db_tweets = db.tweets
    tweets = db_tweets.find({"symbol": symbol}, {"_id":0, "id":0, "score":0})
    return tweets

# Gets the positive, negative and neutral sentiment count
def get_sentiment_count(symbol, db):
    db_sentiment_count = db.sentiment_count
    count = db_sentiment_count.find_one({"symbol":symbol},{"_id":0},sort=[("$natural", -1)])
    return count

# Gets the sentiment trend
def get_sentiment_trend_today(symbol, db):
    db_sentiment_count = db.sentiment_count
    counts = db_sentiment_count.find({"symbol":symbol},{"_id":0})
    return counts

# Gets the change in sentiment
def get_sentiment_change(symbol, db):
    db_prediction = db.prediction
    print symbol
    sentiment = db_prediction.find_one({"symbol":symbol}, {"symbol":1, "sentiment_change":1, "_id":0})
    print sentiment
    return sentiment

# Returns the sentiments of all symbols
def get_sentiments(db):
    db_sentiments = db.sentiments
    all_sentiments = db_sentiments.find({},{"_id":0})
    return all_sentiments

# Returns the sentiment
def get_sentiment(symbol, db):
    db_sentiments = db.sentiments
    all_sentiments = db_sentiments.find_one({"symbol":symbol},{"_id":0})
    return all_sentiments

# Return the predicted change in stock values
def get_predicted_stock_change(symbol, db):
    db_prediction = db.prediction
    predicted_change = 0
    prediction = db_prediction.find_one({"symbol":symbol})
    if prediction != None:
        predicted_change = prediction['multiplication_factor'] * prediction['sentiment_change']
    return predicted_change
