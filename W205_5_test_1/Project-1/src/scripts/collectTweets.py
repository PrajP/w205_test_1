import argparse
import sys
import os
import jsonpickle
import tweepy
from tweepy.auth import OAuthHandler
import datetime
 
#add a parser for search strings
parser = argparse.ArgumentParser(description='Parse the search strings.')
parser.add_argument('-q', '--queryString', action="store", dest="queryString", metavar='QueryStrings', nargs='+', help='Enter the Search Query strings separated by space: -q Samsung galaxy iPhone...')
parser.add_argument('-o', '--outputName', action="store", dest="output", metavar='outputFileName', nargs=1, help='Enter the Output File Name for the search')
args = parser.parse_args()
				
print "Search terms are: "+" ".join(args.queryString)
print "Output file name is: " + args.output[0]

#define search query string
queryString_kw = " OR ".join(['"%s"' % s for s in args.queryString])
queryString_hash = " OR ".join(['#%s' % s for s in args.queryString])
#searchQuery = 'place:96683cc9126741d1 #samsung OR "samsung" OR "SamSung" OR "Galaxy" OR "galaxy phone"'

searchQuery = 'place:96683cc9126741d1 ' + queryString_kw + " OR " + queryString_hash

#define output file name
year_date = str(datetime.datetime.now()).split(' ')[0]
fName = args.output[0]+'_SmartPhone_tweets_'+year_date+'.json' # We'll store the tweets in a text file.

#access keys					
consumer_key = 	'irBLC6hrDRi0Qjbd1OZb3Tvke'
consumer_secret = 'oL3Ta2hJItV7BkZeoA9a1dtZWTMrW38aQ68IVdiQRqUlwOnt9u'
access_token = '788174119955116033-GMbdhBRFXHrJHDj1UbkuC5fEVZ2J44W'
access_secret = 'jL1q2eK8h63BhZKiAHlohQzF9E5ZeNXbHDVwkuyb9Fnlh'


if __name__ == "__main__": 
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
 
	api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify = True)
					   
	if (not api):
		print "Problem connecting to API."
			  	
	maxTweets = 10000000 # Some arbitrary large number
	tweetsPerQry = 100  # this is the max the API permits

	# If results from a specific ID onwards are reqd, set since_id to that ID.
	# else default to no lower limit, go as far back as API allows
	sinceId = None

	# If results only below a specific ID are, set max_id to that ID.
	# else default to no upper limit, start from the most recent tweet matching the search query.
	max_id = -1L

	tweetCount = 0
	print("Downloading max {0} tweets".format(maxTweets))
	with open(fName, 'w') as f:
		while tweetCount < maxTweets:
			try:
				if (max_id <= 0):
					if (not sinceId):
						new_tweets = api.search(q=searchQuery, lang = "en", count=tweetsPerQry)
					else:
						new_tweets = api.search(q=searchQuery, lang = "en", count=tweetsPerQry,
												since_id=sinceId)
				else:
					if (not sinceId):
						new_tweets = api.search(q=searchQuery, lang = "en", count=tweetsPerQry,
												max_id=str(max_id - 1))
					else:
						new_tweets = api.search(q=searchQuery, lang = "en", count=tweetsPerQry,
												max_id=str(max_id - 1),
												since_id=sinceId)
				if not new_tweets:
					print("No more tweets found")
					break
				for tweet in new_tweets:
					f.write(jsonpickle.encode(tweet._json, unpicklable=False) +
							'\n')
				tweetCount += len(new_tweets)
				print("Downloaded {0} tweets".format(tweetCount))
				max_id = new_tweets[-1].id
			except tweepy.TweepError as e:
				# Just exit if any error
				print("some error : " + str(e))
				break

	print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))