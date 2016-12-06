#tweet_load.py
"""
Using Twitter stream API, insert tweets to mongodb
"""

import sys
import os
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
#from Twittercredentials import *
from time import time,ctime
#import simplejson
from pymongo import MongoClient
import json
import io
import os.path

# tweet_load_new.py

def get_db():
	from pymongo import MongoClient
	client = MongoClient('localhost:27017')
	db = client.twitter_db
	collection = db.twitter_collection
	return collection

def add_json(db):

	jasonfiles = ['apple_merged.json', 'samsung_merged.json', 'google_merged.json'] 
	fullpath_1 = []
	my_list = []
	count = 0

	source = '/data/rawdata'
	for root, dirs, filenames in os.walk(source):
		for f in jasonfiles:
			

			f_name = f[:-12]
			source_new=source+'/'+ f_name+ '/'
			fullpath_new = os.path.join(source_new, f)
			fullpath_1.append(fullpath_new)
			my_list = list(set(fullpath_1))


	for jsonfile in my_list:
		tweet = []
		with open(jsonfile,'r') as data:
			for line in data:	
			
       				tweet.append(json.loads(line))
				count += 1
				#tweet.append(json.loads(line))	


				db.twitter_d_collection.insert_one(json.loads(line))
       				#db.twitter_collection.insert(json.loads(line))
				#tweet = json.load(data)

				#db.twitter_d_collection.insert_one(tweet)
		print jsonfile, count
    
def get_tweet(db):
    #return db.twitter_collection.find_one()
    return db.twitter_collections.find({"user.screen_name":"nytimes"}).sort("_id",1).limit(1)
    #return db.twitter_collections.find({"user.screen_name":"nytimes"}).limit(1)
    
if __name__ == "__main__":

    db = get_db() 
    add_json(db)
 
    #print get_tweet(db)
    
 
