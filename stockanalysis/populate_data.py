'''
Created on Apr 19, 2015

@author: rajiv
'''

#from tweetRepository import delete_tweets, process_tweets, store_tweets,store_sentiment
import tweetRepository as tr
import datetime

def populate():
    #while True:
    

    
        tr.delete_tweets()
        for symbol in mySymbols:
            tr.process_tweets(symbol, 100)
        tr.store_tweets()
        print datetime.datetime.now().time()
        
        for symbol in mySymbols:
            tr.store_sentiment(symbol)        
            tr.store_top_tweets(symbol)

        


mySymbols = {"$AAPL", "$BAC", "$GE", "$CMCSA", "$MSFT", "$CSCO", "$F", "$INTC", "$T", "$PFE"}
populate()