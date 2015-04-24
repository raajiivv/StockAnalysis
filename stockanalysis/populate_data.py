'''
Created on Apr 19, 2015

@author: rajiv
'''

#from tweetRepository import delete_tweets, process_tweets, store_tweets,store_sentiment
import tweetRepository as tr
import datetime, time, requests

def populate():
    date = int(datetime.date.today().strftime("%d"))
    while True:
        h = datetime.datetime.utcnow().hour
        w = datetime.datetime.today().weekday()
        d = int(datetime.date.today().strftime("%d"))

        # Does not populate on weekends
        if w == 5 or w == 6:
            continue
        # Does not populate after working hours
        if h< 13 or h> 20:
            continue
        
        if d!=date:
            tr.delete_sentiment_count()
            
            print date, d
            date = d
        
        for symbol in mySymbols:
            tr.process_tweets(symbol, 100)
        tr.delete_tweets()
        tr.store_tweets()
        print datetime.datetime.now().time()
        
        for symbol in mySymbols:
            tr.store_sentiment(symbol)        
            tr.store_sentiment_count(symbol)
            tr.store_top_tweets(symbol)
            
        # Waits for 15 minutes before populating the data again
        time.sleep(900)        
            
mySymbols = set()
results = requests.get(url="http://54.191.103.141:8800/getSymbols")
print results
response = results.json()
for symbol in response:
    mySymbols.add(symbol['symbol'])

#mySymbols = {"$AAPL", "$BAC", "$GE", "$CMCSA", "$MSFT", "$CSCO", "$F", "$INTC", "$T", "$PFE"}
populate()