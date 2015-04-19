'''
Created on Apr 16, 2015

@author: rajiv
'''
import datetime
from multiprocessing import Pool, Lock, Queue, Manager
import sys
from ystockquote import get_price


import pymongo
import requests, json, urllib, base64

import dao


def get_credentials():
    creds = {}
    creds['consumer_key'] = str()
    creds['consumer_secret'] = str()
    creds['apikey'] = str()
    creds['mongo_server'] = str()
    creds['mongo_port'] = str()
    creds['db_username'] = str()
    creds['db_password'] = str()
    

    # If the file credentials.py exists, then grab values from it.
    # Otherwise, the values are entered by the user
    try:
        import credentials
        creds['consumer_key'] = credentials.twitter_consumer_key
        creds['consumer_secret'] = credentials.twitter_consumer_secret
        creds['apikey'] = credentials.alchemy_apikey
        creds['mongo_server'] = credentials.mongo_server
        creds['mongo_port'] = credentials.mongo_port
        creds['db_username'] = credentials.db_username
        creds['db_password'] = credentials.db_password
    except:
        print "No credentials.py found"
        creds['consumer_key'] = raw_input("Enter your Twitter API consumer key: ")
        creds['consumer_secret'] = raw_input("Enter your Twitter API consumer secret: ")
        creds['apikey'] = raw_input("Enter your AlchemyAPI key: ")
        creds['mongo_server'] = '127.0.0.1'
        creds['mongo_port'] = 27017
        
    return creds

def oauth(credentials):

    print "Requesting bearer token from Twitter API"

    try:
        # Encode credentials
        encoded_credentials = base64.b64encode(credentials['consumer_key'] + ':' + credentials['consumer_secret'])        
        # Prepare URL and HTTP parameters
        post_url = "https://api.twitter.com/oauth2/token"
        parameters = {'grant_type' : 'client_credentials'}
        # Prepare headers
        auth_headers = {
            "Authorization" : "Basic %s" % encoded_credentials,
            "Content-Type"  : "application/x-www-form-urlencoded;charset=UTF-8"
            }

        # Make a POST call
        results = requests.post(url=post_url, data=urllib.urlencode(parameters), headers=auth_headers)
        response = results.json()

        # Store the access_token and token_type for further use
        auth = {}
        auth['access_token'] = response['access_token']
        auth['token_type'] = response['token_type']

        print "Bearer token received"
        return auth

    except Exception as e:
        print "Failed to authenticate with Twitter credentials:", e
        print "Twitter consumer key:", credentials['consumer_key']
        print "Twitter consumer secret:", credentials['consumer_secret']
        sys.exit()
        

def search(search_term, num_tweets, auth):
    # This collection will hold the Tweets as they are returned from Twitter
    collection = []
    # The search URL and headers
    url = "https://api.twitter.com/1.1/search/tweets.json"
    search_headers = {
        "Authorization" : "Bearer %s" % auth['access_token']
        }
    max_count = 100
    next_results = ''
    # Can't stop, won't stop
    while True:
        #print "Search iteration, Tweet collection size: %d" % len(collection)
        count = min(max_count, int(num_tweets) - len(collection))

        # Prepare the GET call
        if next_results:
            get_url = url + next_results
        else:
            parameters = {
                'q' : search_term,
                'count' : count,
                'lang' : 'en'
                } 
            get_url = url + '?' + urllib.urlencode(parameters)

        # Make the GET call to Twitter
        results = requests.get(url=get_url, headers=search_headers)
        response = results.json()

        # Loop over statuses to store_tweets the relevant pieces of information
        for status in response['statuses']:
            text = status['text'].encode('utf-8')

            # Filter out retweets
            if status['retweeted'] == True:
                continue
            if text[:3] == 'RT ':
                continue

            tweet = {}
            # Configure the fields you are interested in from the status object
            tweet['text'] = text
            tweet['id'] = status['id']
            tweet['time'] = status['created_at'].encode('utf-8')
            tweet['screen_name'] = status['user']['screen_name'].encode('utf-8')
            tweet['symbol'] = search_term
            
            collection += [tweet]
        
            if len(collection) >= num_tweets:
                print "Search complete! Found %d tweets" % len(collection)
                return collection

        if 'next_results' in response['search_metadata']:
            next_results = response['search_metadata']['next_results']
        else:
            print "Uh-oh! Twitter has dried up. Only collected %d Tweets (requested %d)" % (len(collection), num_tweets)
            print "Last successful Twitter API call: %s" % get_url
            print "HTTP Status:", results.status_code, results.reason
            return collection

def enrich(credentials, tweets, sentiment_target=''):
    # Prepare to make multiple asynchronous calls to AlchemyAPI
    apikey = credentials['apikey']
    pool = Pool(processes=10)
    mgr = Manager()
    result_queue = mgr.Queue()
    # Send each Tweet to the get_text_sentiment function
    for tweet in tweets:
        pool.apply_async(get_text_sentiment, (apikey, tweet, sentiment_target, result_queue))

    pool.close()
    pool.join()
    collection = []
    while not result_queue.empty():
        collection += [result_queue.get()]
    
    print "Enrichment complete! Enriched %d Tweets" % len(collection)
    return collection

def get_text_sentiment(apikey, tweet, target, output):

    # Base AlchemyAPI URL for targeted sentiment call
    alchemy_url = "http://access.alchemyapi.com/calls/text/TextGetTextSentiment"
    
    # Parameter list, containing the data to be enriched
    parameters = {
        "apikey" : apikey,
        "text"   : tweet['text'],
        "outputMode" : "json",
        "showSourceText" : 1
        }

    try:

        results = requests.get(url=alchemy_url, params=urllib.urlencode(parameters))
        response = results.json()

    except Exception as e:
        print "Error while calling TextGetTargetedSentiment on Tweet (ID %s)" % tweet['id']
        print "Error:", e
        return

    try:
        if 'OK' != response['status'] or 'docSentiment' not in response:
            #print "Problem finding 'docSentiment' in HTTP response from AlchemyAPI"
            #print response
            #print "HTTP Status:", results.status_code, results.reason
            #print "--"
            return

        tweet['sentiment'] = response['docSentiment']['type']
        tweet['score'] = 0.
        if tweet['sentiment'] in ('positive', 'negative'):
            tweet['score'] = float(response['docSentiment']['score'])
        output.put(tweet)

    except Exception as e:
        print "D'oh! There was an error enriching Tweet (ID %s)" % tweet['id']
        print "Error:", e
        print "Request:", results.url
        print "Response:", response
    return

def dedup(tweets):
    used_ids = []
    collection = []
    for tweet in tweets:
        if tweet['id'] not in used_ids:
            used_ids += [tweet['id']]
            collection += [tweet]
    print "After de-duplication, %d tweets" % len(collection)
    return collection


def getTopTweets(sym):
    topTweets = []
    topPositive = {"symbol": "$AAPL", "timestamp": "Fri Apr 17 04:45:15 +0000 2015", "text": "$OSLH Has Become One Of The Most Powerful Penny Stocks This Week! Special Update: http://t.co/rQZs5De8kI $TRX $AAPL $GOOG", "screen_name":"StockDawgzz"}
    topNegative = {"symbol": "$AAPL", "timestamp": "Fri Apr 17 02:39:53 +0000 2015", "text": "Analysts Harsh On SanDisks Q1 Suffering $AAPL $SNDK http://t.co/PFErzmdzen", "screen_name":"leegarcia121"}
    topTweets.append(topPositive)
    topTweets.append(topNegative)
    topTweets = json.dumps(topTweets, ensure_ascii = False)
    print topTweets
    return topTweets

def process_tweets(search_term, num_tweets):
    # Pull Tweets down from the Twitter API
    raw_tweets = search(search_term, num_tweets, auth)
    print datetime.datetime.now().time()
    
    # De-duplicate Tweets by ID
    unique_tweets = dedup(raw_tweets)
    print datetime.datetime.now().time()
    
    # Enrich the body of the Tweets using AlchemyAPI
    global enriched_tweets
    enriched_tweets = enriched_tweets + enrich(credentials, unique_tweets, sentiment_target=search_term)
    print datetime.datetime.now().time()
    
def store_tweets():
    # Store data in MongoDB
    dao.store_tweets(enriched_tweets, db)
    print datetime.datetime.now().time()
    return    
    
def delete_tweets():
    # Delete data from MongoDB
    dao.delete_tweets(db)
    print datetime.datetime.now().time()
    return
    
def store_sentiment(symbol):
    stock_value = get_price(symbol.strip("$"))
    dao.store_sentiment(symbol, db, stock_value)
    print datetime.datetime.now().time()
    return

def store_top_tweets(symbol):
    dao.store_top_tweets(symbol, db)
    return
    
    
# Establish credentials for Twitter, AlchemyAPI and MongoDB
credentials = get_credentials()

# Get the Twitter bearer token
auth = oauth(credentials)

# Get the MongoDB database and authenticate
db = pymongo.MongoClient(credentials['mongo_server'], credentials['mongo_port']).stockanalysis
if credentials['db_username'] != '':  
    db.authenticate(credentials['db_username'], credentials['db_password'])
    

enriched_tweets = []

