import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import re
import sys
import argparse
import os
import glob
from pymongo import MongoClient
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def parse_args():
	#add a parser for search strings
	parser = argparse.ArgumentParser(description='Parse the search strings.')
	parser.add_argument('-d', '--directory', action="store", dest="directory", metavar='DIR', nargs=1,
						help='Enter the path to the directory with json files : -d DIR')
						
	parser.add_argument('-o', '--outfilename', action="store", dest="outfilename", metavar='OUTFILENAME', nargs=1,
						help='Enter the output file name for the csv file : -o OUTFILENAME')					
						
	opts = parser.parse_args()
	
	if not (opts.directory or opts.outfilename):
		parser.error("You have to specify either a --directory or --outfilename!")
		
	outfile = os.path.join(opts.directory[0], opts.outfilename[0])
		
	print "Directory to read in json files is: "+ opts.directory[0] + " and File name to output is: "+ outfile
	return opts

def readJson(file):
    '''read in a json file and return a df
    '''
    with open(file, 'r') as f:
        data = f.readlines()
    
    data_json_str = '[' + ','.join(data).replace('\n', '') + ']'
    return pd.read_json(data_json_str)


def get_merged_json(flist, **kwargs):
    return pd.concat([readJson(f) for f in flist], **kwargs)
	
def dfCleanUp(df):
	'''
		flatten 'user' and 'place' field and clean up data frame
	'''
	#remove rows that do not have values in 'user', 'place', and 'geo'
	#rows_keep = [i for i in range(len(df['user'])) if df.user[i] and df.place[i] and df.geo[i]]
	
	#df = df.iloc[rows_keep, ]
	#print 'The dimension of data frame (cleaned up) is ' + str(df.shape[0]) + ' x ' + str(df.shape[1])
	user_df = json_normalize(df.user)
	user_df.columns = ['user.' + s for s in user_df.columns.values]
	df = pd.concat([df, user_df], axis = 1)

	#process 'place' column and concatenate to original df
	place_filled = []
	for i in range(len(df.place)):
		place_item = df.place[i]
		if place_item:
			place_filled.append(place_item)
		else:
			dummy_place = {'country': None, 'country_code': None, 'full_name': None,'id': None, 'name': None, 'place_type': None, 'url': None}
			place_filled.append(dummy_place)
		
	df.place = place_filled
	place_df = json_normalize(df.place)
	place_df.columns = ['place.' + s for s in place_df.columns.values]
	
	place_city_state = place_df['place.full_name'].str.split(',', expand = True)
	
	place_city_state_column_names = []
	for i in range(len(place_city_state.columns)):
		if i ==0:
			place_city_state_column_names.append("place.city")
		elif i==1:
			place_city_state_column_names.append("place.state")
		else:
			delCol = "delete" + str(i)
			place_city_state_column_names.append(delCol)
	
	place_city_state.columns = place_city_state_column_names
	
	#keep only first two
	place_city_state = place_city_state[['place.city','place.state']]
	place_city_state['place.city'] = place_city_state['place.city'].str.strip()
	place_city_state['place.state'] = place_city_state['place.state'].str.strip()
	
	#drop place.full_name and concat the parsed columns
	place_df = place_df.drop(['place.full_name', 'place.name'],1)
	place_df = pd.concat([place_df, place_city_state], axis =1)
		
	df = pd.concat([df, place_df], axis = 1)

	#df.head(5).to_csv(outfile, header = True, index = False)

	#print(df.columns.values)
	
	#parse out geo column split into latitude and longtitude
	latitude = []
	longtitude = []
	hashtags = []
	
	for i in range(len(df.geo)):
		#print(geo_item['coordinates'])
		geo_item = df.geo[i]
		
		if geo_item:
			if 'coordinates' in geo_item.keys():
				latitude.append(geo_item['coordinates'][0])
				longtitude.append(geo_item['coordinates'][1])
			else:
				latitude.append(None)
				longtitude.append(None)
		else:
			latitude.append(None)
			longtitude.append(None)
		
		#parse out hashtags from entities field and concatenate into a comma separated string
		entities_item = df.entities[i]
		
		if entities_item:
			if 'hashtags' in entities_item.keys():
				hashtag_str = ""
				for j in entities_item['hashtags']:
					hashtag_str += j['text'] +"|"
				hashtags.append(hashtag_str)
			else:
				hashtags.append(None)
		else:
			hashtags.append(None)
		
	df['geo.latitude'] = latitude
	df['geo.longtitude'] = longtitude
	df['hashtags'] = hashtags
	

	#drop the unwanted columns
	df = df.drop(['_id','contributors','coordinates','metadata','source','entities','extended_entities','geo','place','user','user.profile_background_color','user.profile_background_image_url','user.profile_background_image_url_https',
	'user.profile_background_tile','user.profile_banner_url', 'user.profile_image_url',
	'user.profile_image_url_https', 'user.profile_link_color',
	'user.profile_sidebar_border_color', 'user.profile_sidebar_fill_color',
	'user.profile_text_color','user.profile_use_background_image',
	'user.description','user.entities.description.urls','user.entities.url.urls',
	'place.bounding_box.coordinates','place.bounding_box.type','place.contained_within',
	'in_reply_to_screen_name','in_reply_to_status_id','in_reply_to_status_id_str',
	'in_reply_to_user_id','in_reply_to_user_id_str'], 1)
	
	return df
	
	
def applyFilters(df):
	#apply filters
	#1. English only
	print "filtering out non-English tweets.."
	print str(len(df) - len(df[(df['user.lang'] == 'en')])) + " rows filtered."
	df = df[(df['user.lang'] == 'en')]
	print "done."
	print df.shape
	
	#2. retweeted = False
	print "filtering out retweets..."
	print str(len(df) - len(df[(df.retweeted == False)])) + " rows filtered."
	df = df[(df.retweeted == False)]
	print "done."
	print df.shape

	#3. only one user per tweet
	print "filtering out duplicate tweets from the same user..."
	print str(len(df) - len(df.drop_duplicates(subset = 'user.id', keep = 'first'))) + " rows filtered."
	df = df.drop_duplicates(subset = 'user.id', keep = 'first')
	print "done."
	print df.shape

	#4. keyword filtering
	print "filtering out tweets by key words..."
	excludes = ['Milky Way', 'LA Galaxy', 'Guardians of the galaxy']
	pat = '|'.join(map(re.escape, excludes))
	cond = df.text.str.contains(pat, case = False)
	print str(len(df) - len(df.drop(df[cond].index))) + " rows filtered."
	df = df.drop(df[cond].index)
	print "done."
	print df.shape
	
	return df
	
	
def writeDFtoCSV(df, outfilename):
	df.to_csv(outfilename, header = True, index = False)
	
def sentimentScoring(sentence):
	sid = SentimentIntensityAnalyzer()
	ss = sid.polarity_scores(sentence)
	
	return ss

	
def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]


def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id:
        del df['_id']

    return df
	
	
def main():
	reload(sys)
	sys.setdefaultencoding('utf-8')

	companies = ['apple', 'google', 'samsung']
	db_names = ['twitter_apple_db', 'twitter_google_db', 'twitter_samsung_db']
	collection_names = ['twitter_apple_collection', 'twitter_google_collection', 'twitter_samsung_collection']
	
	#output dir
	outPath = "/data/analysis_output"
	
	for i in range(len(companies)):	
		print "Retrieving "+ companies[i] + " collection from db..."
		
		
		outCSV = outPath+"/"+companies[i]+"_result.csv"
		
		#connect to mongo
		client = MongoClient()
		db = client[db_names[i]]
		collection = db[collection_names[i]]
		df = pd.DataFrame(list(collection.find()))
		
		print 'The dimension of data frame is ' + str(df.shape[0]) + ' x ' + str(df.shape[1])
		
		#print df.head(5)
		#sys.exit()


		###################################
		#if reading from raw json files
			#opts = parse_args()
			#if opts.debug:
			#	logging.basicConfig(level=logging.DEBUG)
			#else:
			#	logging.basicConfig(level=logging.INFO)
				
			#get all the json files in the directory and aggregate in one df
			#fmask = os.path.join(opts.directory[0], '*.json')
			#df = get_merged_json(glob.glob(fmask),ignore_index=True)
		###################################	
			
		df_norm = dfCleanUp(df)
		df_norm = applyFilters(df_norm)
	
		#apply sentiment scoring
		print "Applying sentiment analysis..."
		
		compound=[]
		pos=[]
		neg=[]
		neu=[]
		for sentence in df_norm.text:
			ss = sentimentScoring(sentence)
			compound.append(ss['compound'])
			pos.append(ss['pos'])
			neg.append(ss['neg'])
			neu.append(ss['neu'])
		
		df_norm['ss_compound'] = compound
		df_norm['ss_pos'] = pos
		df_norm['ss_neg'] = neg
		df_norm['ss_neu'] = neu
		
		#add the brand company name to field
		df_norm['brand'] = companies[i]
			
			#print(sentence)
			#print(ss)
		
		#output to csv
		print "Writing to csv..."
		writeDFtoCSV(df_norm, outCSV)
		
		print "done."
		
		client.close()

if __name__ == "__main__": 
	main()