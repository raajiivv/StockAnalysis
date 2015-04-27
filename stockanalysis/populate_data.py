'''
Created on Apr 19, 2015

@author: rajiv
'''

import tweetRepository as tr
import datetime, time, requests

def populate():
    global date
    while True:
        h = datetime.datetime.utcnow().hour
        w = datetime.datetime.today().weekday()
        d = int(datetime.date.today().strftime("%d"))

        # Does not populate on weekends
        if w == 5 or w == 6:
            if h == 12 or h == 23:
                populate_db(d, 1000)
                time.sleep(3600)
            continue
        # Does not populate after working hours
        if h< 13 or h> 20:
            continue
        
        populate_db(d, 100)
        # Waits for 15 minutes before populating the data again
        time.sleep(900)

def populate_db(d, count):
    global date
    if d!=date:
        tr.delete_sentiment_count()
        print date, d, count
        date = d
    
    for symbol in mySymbols:
        tr.process_tweets(symbol, count)
    tr.delete_tweets()
    tr.store_tweets()
    print datetime.datetime.now().time()
    
    for symbol in mySymbols:
        tr.store_sentiment(symbol)        
        tr.store_sentiment_count(symbol)
        tr.store_top_tweets(symbol)
        
            
mySymbols = set()
results = requests.get(url="http://54.191.103.141:8800/getSymbols")
response = results.json()
for symbol in response:
    mySymbols.add(symbol['symbol'])
date = int(datetime.date.today().strftime("%d"))
populate()