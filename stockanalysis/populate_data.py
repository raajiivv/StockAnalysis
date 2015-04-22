'''
Created on Apr 19, 2015

@author: rajiv
'''

#from tweetRepository import delete_tweets, process_tweets, store_tweets,store_sentiment
import tweetRepository as tr
import datetime, time, requests

def populate():
    h = datetime.datetime.utcnow().hour
    w = datetime.datetime.today().weekday()
    while True:
        # Waits for 15 minutes before populating the data again
        time.sleep(900)
        # Does not populate on weekends
        if w == 5 or w == 6:
            continue
        # Does not populate after working hours
        if h< 13 or h> 20:
            continue
        
        for symbol in mySymbols:
            tr.process_tweets(symbol, 50)
        tr.delete_tweets()
        tr.store_tweets()
        print datetime.datetime.now().time()
        
        for symbol in mySymbols:
            tr.store_sentiment(symbol)        
            tr.store_top_tweets(symbol)
            
results = requests.get(url="http://54.191.103.141:8800/getSymbols")
response = results.json()
mySymbols = response['symbols']

#mySymbols = {"$AAPL", "$BAC", "$GE", "$CMCSA", "$MSFT", "$CSCO", "$F", "$INTC", "$T", "$PFE"}
populate()