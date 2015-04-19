'''
Created on Apr 19, 2015

@author: rajiv
'''

from tweetRepository import delete_tweets, process_tweets, store_tweets,store_sentiment
import datetime

def populate():
    #while True:
    

    
        delete_tweets()
        for symbol in mySymbols:
            process_tweets(symbol, 100)
        store_tweets()
        print datetime.datetime.now().time()
        
        for symbol in mySymbols:
            store_sentiment(symbol)        
        
        


mySymbols = {"$AAPL", "$BAC", "$GE", "$CMCSA", "$MSFT", "$CSCO", "$F", "$INTC", "$T", "$PFE"}
populate()