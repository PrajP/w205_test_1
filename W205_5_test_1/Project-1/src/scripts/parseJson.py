import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import re
import sys
import argparse
import os
import glob

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
		
	print("Directory to read in json files is: "+ opts.directory[0] + " and File name to output is: "+ outfile)
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
	user_df = json_normalize(df.user)
	user_df.columns = ['user.' + s for s in user_df.columns.values]
	df = pd.concat([df, user_df], axis = 1)

	#process 'place' column and concatenate to original df
	place_df = json_normalize(df.place)
	place_df.columns = ['place.' + s for s in place_df.columns.values]
	df = pd.concat([df, place_df], axis = 1)

	#df.head(5).to_csv(outfile, header = True, index = False)

	#print(df.columns.values)

	#drop the unwanted columns
	df = df.drop(['coordinates','metadata','place','user','user.profile_background_color',
							'user.profile_background_image_url','user.profile_background_image_url_https',
							'user.profile_background_tile','user.profile_banner_url', 'user.profile_image_url',
	 'user.profile_image_url_https', 'user.profile_link_color',
	 'user.profile_sidebar_border_color', 'user.profile_sidebar_fill_color',
	 'user.profile_text_color','user.profile_use_background_image'], 1)
	
	return df
	
	
def applyFilters(df):
	#apply filters
	#1. retweeted = False
	print("filtering out retweets...")
	print(str(len(df) - len(df[(df.retweeted == False)])) + " rows filtered.")
	df = df[(df.retweeted == False)]
	print("done.")
	print(df.shape)

	#2. only one user per tweet
	print("filtering out duplicate tweets from the same user...")
	print(str(len(df) - len(df.drop_duplicates(subset = 'user.id', keep = 'first'))) + " rows filtered.")
	df = df.drop_duplicates(subset = 'user.id', keep = 'first')
	print("done.")
	print(df.shape)

	#3. noise filtering
	print("filtering out tweets by key words...")
	excludes = ['Milky Way', 'LA Galaxy', 'Guardians of the galaxy']
	pat = '|'.join(map(re.escape, excludes))
	cond = df.text.str.contains(pat, case = False)
	print(str(len(df) - len(df.drop(df[cond].index))) + " rows filtered.")
	df = df.drop(df[cond].index)
	print("done.")
	print(df.shape)	
	
	return df
	

def writeDFtoCSV(df, outfilename):
	df.to_csv(outfilename, header = True, index = False)
	
def main():
	opts = parse_args()
	
	#if opts.debug:
	#	logging.basicConfig(level=logging.DEBUG)
	#else:
	#	logging.basicConfig(level=logging.INFO)
		
	#get all the json files in the directory and aggregate in one df
	fmask = os.path.join(opts.directory[0], '*.json')
	df = get_merged_json(glob.glob(fmask),ignore_index=True)
	
	df_norm = dfCleanUp(df)
	df_norm = applyFilters(df_norm)
	
	#output to csv
	writeDFtoCSV(df_norm, opts.outfilename[0])
	

if __name__ == "__main__": 
	main()