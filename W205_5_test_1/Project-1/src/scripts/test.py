#!/usr/local/bin/python2.7
import sys
import os
import jsonpickle
import tweepy
from tweepy.auth import OAuthHandler
 
#reload(sys)
#sys.setdefaultencoding('utf-8')

consumer_key = 	'irBLC6hrDRi0Qjbd1OZb3Tvke'
consumer_secret = 'oL3Ta2hJItV7BkZeoA9a1dtZWTMrW38aQ68IVdiQRqUlwOnt9u'
access_token = '788174119955116033-GMbdhBRFXHrJHDj1UbkuC5fEVZ2J44W'
access_secret = 'jL1q2eK8h63BhZKiAHlohQzF9E5ZeNXbHDVwkuyb9Fnlh'

if __name__ == "__main__": 
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
 
	api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify = True,
                       retry_count = 5, #retry 5 times
                       retry_delay = 5) #seconds to wait for retry

	#print our own status updates
	#for status in tweepy.Cursor(api.home_timeline).items(10):
	#Process a single status
	#print status.text

	#public_tweets = api.home_timeline()
	#for tweet in public_tweets:
	#	print tweet.text

	#print the top trends
	#trends1 = api.trends_place(1) # from the end of your code
	# trends1 is a list with only one element in it, which is a 
	# dict which we'll put in data.
	#data = trends1[0] 
	# grab the trends
	#trends = data['trends']
	# grab the name from each trend
	#names = [trend['name'] for trend in trends]
	# put all the names together with a ' ' separating them
	#trendsName = ' '.join(names)
	#print trendsName

	#searchQuery = '#iPhone OR #Apple OR #Samsung OR' \
    #          '#Pixel OR #Google' \
    #          '"iPhone" OR "iphone" OR "samsung" OR "galaxy"'
			  
	searchQuery = '"iPhone" OR "iphone" OR "samsung" OR "SamSung" OR "Galaxy" OR "galaxy phone" OR "pixel phone" OR "Apple" OR "Google"'

	#Maximum number of tweets we want to collect 
	maxTweets = 1000000

	#The twitter Search API allows up to 100 tweets per query
	tweetsPerQry = 100

	tweetCount = 0

	#Open a text file to save the tweets to
	with open('Smart_Phones_Tweets.json', 'w') as f:

	#Tell the Cursor method that we want to use the Search API (api.search)
    #Also tell Cursor our query, and the maximum number of tweets to return
		for tweet in tweepy.Cursor(api.search,q=searchQuery, lang="en").items(maxTweets) :         

			#Verify the tweet has place info before writing (It should, if it got past our place filter)
			if tweet.place is not None:
            
				#Write the JSON format to the text file, and add one to the number of tweets we've collected
				f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
				tweetCount += 1

    #Display how many tweets we have collected
 	print "Downloaded {0} tweets".format(tweetCount)

