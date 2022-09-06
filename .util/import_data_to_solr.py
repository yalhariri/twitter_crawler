#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import gzip
import nltk
import yaml
import shutil
import atexit
import pysolr
from util import *
import logging
tokenizer = nltk.TweetTokenizer()
from datetime import date, datetime
from os import path, listdir, remove
from os.path import isfile, exists, join
status_file = 'status_file_from_files'
from logging.handlers import TimedRotatingFileHandler

selected_fields = True
TWEET_LIST_LIMIT = 20000
CURRENT = 0
registered = 0

def writeToSolr(tweets, core, file):
	"""A function to write the tweets object to file and to solr.

	Args:
		tweets (dict): A dictionary that holds all the relevant information from the tweets.
	"""
	today = date.today()
	d4 = today.strftime("%b_%d_%Y")
	file_name = f'import_to_solr_{core}_{d4}.json'
	missed_data_written = 0
	Error_occured = True
	#headers = {'content-type': 'application/json; charset=utf-8'}
	tweets_list = [tweets[k] for k in tweets.keys()]

	for item in tweets_list:
		if "_version_" in item.keys():
			item.pop('_version_')

	tweets_list_t = tweets_list[0:TWEET_LIST_LIMIT]
	tweets_list = tweets_list[TWEET_LIST_LIMIT:]
	registered = 0
	while(len(tweets_list_t)>0):
		try:
			status = ''
			i = 0
			while ('"status">0<' not in status and i < 3):
				try:
					status = solr.add(tweets_list_t, softCommit=False, fieldUpdates={ 'attr_replies_times':'add-distinct', 'attr_replies_tweets':'add-distinct',  'attr_quote_tweets':'add-distinct', 'quotation_id':'add-distinct', 'attr_quote_times':'add-distinct', 'attr_quoters':'add-distinct','quote_count_i':'set', 'attr_retweet_times':'add-distinct', 'users_followers_count':'set','users_friends_count':'set', 'retweet_count':'set', 'favorite_count':'set','retweeters':'add-distinct', 'hashtags':'add-distinct', 'language':'set','language_twitter':'set', 'reply_count':'set'})
					if '"status">0<' not in status:
						Error_occured = True
					else:
						Error_occured = False
						registered += len(tweets_list_t)
				except Exception as exp:
					if '"status">0<' not in status:
						Error_occured = True
				finally:
					i+=1
			if Error_occured:
				print(f'Error at sending data to solr1, with file: {file}.') #can we make it auto?
				logger.warning(f'Error at sending data to solr1, with file: {file}.')
				writeDataToFile(tweets_list_t, file_name, missed_data_folder)
				missed_data_written = 1
		except Exception:
			if missed_data_written != 1 and Error_occured:
				print(f'Error at sending data to solr2, with file: {file}.')
				logger.warning(f'Error at sending data to solr2, with file: {file}.')
				writeDataToFile(tweets_list_t, file_name, missed_data_folder)
		finally:
			tweets_list_t =  tweets_list[0:TWEET_LIST_LIMIT]
			tweets_list = tweets_list[TWEET_LIST_LIMIT:]
	
	if Error_occured:
		if missed_data_written == 1:
			logger.info(f'Exception occured and Data written to file {missed_data_folder}/{file_name}.')
			logger.info(f'Total of {registered} tweets written to file:{missed_data_folder}/{file_name}.')
		else:
			return False
	else:
		logger.info('No exception occured, Data sent to solr')
		logger.info(f'Total of {registered} tweets written to solr.')
	return True


if __name__== "__main__":

	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('-c','--config', help="configuration setting file.", default='../.config/.config.yml')
	args = parser.parse_args()
	
	try:
		with open(str(args.config)) as file:
			configs = yaml.load(file, Loader=yaml.FullLoader)
			PROJECT= configs['PROJECT']
			KEY = configs['KEY']
			LOG= configs['LOG']
	except Exception as exp:
		print(exp)
		print(f'Configuration file in the path {config_file} not found.\nPlease add configuration file')
		sys.exit(-1)
	
	if (not os.path.exists(LOG)):
		os.makedirs(LOG)
	
	core = PROJECT
	url_solr = f"{configs['SOLR']}{core}/"
	solr = pysolr.Solr(url=url_solr, auth=(configs['USERNAME'],configs['PASSWORD']),timeout=150)

	cache_folder = configs['CACHE_FOLDER']
	log_folder = configs['LOG']
	missed_data_folder = configs['MISSED_FOLDER']
	processed_folder = configs['PROCESSED_FOLDER']
	config_file = args.config
	
	if not path.exists(log_folder):
		os.mkdir(log_folder)
	if not path.exists(missed_data_folder):
		os.mkdir(missed_data_folder)
	if not path.exists(cache_folder):
		os.mkdir(cache_folder)

	logger = logging.getLogger(__name__)
	filename = f'{log_folder}/{__name__}.log'
	file_handler = TimedRotatingFileHandler(filename=filename, when='midnight', interval=1, backupCount=0)#when midnight, s (seconds), M (minutes)... etc
	formatter  = logging.Formatter('[%(asctime)s : %(levelname)s : %(name)s ]: %(message)s')
	file_handler.setFormatter(formatter)
	logger.addHandler(file_handler)
	logger.setLevel(logging.INFO)
	
	atexit.register(exitHandler, project =PROJECT, tool='Extracting data', key=KEY)
	
	confirmALife(PROJECT,tool='Importing data', key=KEY)

	run = True
	with open (status_file, 'w') as fout:
		fout.write('1')

	main_folder = path.abspath(processed_folder)
	onlyfiles = []
	
	print('counting required files started')
	try:
		for file in listdir(main_folder):
			if isfile(join(main_folder, file)) and not file.endswith('.gz'):
				onlyfiles.append(join(main_folder, file))
	except Exception as exp:
		handleException(exp, file, '__main__ -> counting required files')

	print('reading from files started')

	day = datetime.now()
	limit = day.strftime('%Y_%m_%d')

	for file in sorted(onlyfiles):
		if limit not in file:
			results = False
			try :
				combined_tweets = dict()
				print('working on file', file)
				with open(file, "r", encoding="utf-8") as fin:
					for line in fin:
						tweet_obj = None
						try:
							tweet_obj = json.loads(line)
						except Exception as exp:
							handleException(exp, line, '__main__ -> reading tweets objects from file')
							tweet_obj = None
						
						if tweet_obj != None:
							if 'language' not in tweet_obj.keys():
								tweet_obj['language'] = get_language(tweet_obj['full_text'])
							elif tweet_obj['language'] == "":
								tweet_obj['language'] = get_language(tweet_obj['full_text'])
							if tweet_obj['id'] not in combined_tweets.keys():
								combined_tweets[tweet_obj['id']] = tweet_obj
							else:
								
								if 'attr_quote_tweets' in tweet_obj.keys():
									if 'attr_quote_tweets' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['attr_quote_tweets'] += tweet_obj['attr_quote_tweets']
									else:
										combined_tweets[tweet_obj['id']]['attr_quote_tweets'] = tweet_obj['attr_quote_tweets']
									combined_tweets[tweet_obj['id']]['attr_quote_tweets'] = list(set(combined_tweets[tweet_obj['id']]['attr_quote_tweets']))
								
								if 'attr_quote_times' in tweet_obj.keys():
									if 'attr_quote_times' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['attr_quote_times'] += tweet_obj['attr_quote_times']
									else:
										combined_tweets[tweet_obj['id']]['attr_quote_times'] = tweet_obj['attr_quote_times']
									combined_tweets[tweet_obj['id']]['attr_quote_times'] = list(set(combined_tweets[tweet_obj['id']]['attr_quote_times']))
								
								if 'attr_quoters' in tweet_obj.keys():
									if 'attr_quoters' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['attr_quoters'] += tweet_obj['attr_quoters']
									else:
										combined_tweets[tweet_obj['id']]['attr_quoters'] = tweet_obj['attr_quoters']
									combined_tweets[tweet_obj['id']]['attr_quoters'] = list(set(combined_tweets[tweet_obj['id']]['attr_quoters']))
								
								if 'quote_count_i' in tweet_obj.keys():
									if 'quote_count_i' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['quote_count_i'] = max(combined_tweets[tweet_obj['id']]['quote_count_i'],tweet_obj['quote_count_i'])
									else:
										combined_tweets[tweet_obj['id']]['quote_count_i'] = tweet_obj['quote_count_i']

								if 'attr_retweet_times' in tweet_obj.keys():
									if 'attr_retweet_times' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['attr_retweet_times'] += tweet_obj['attr_retweet_times']
									else:
										combined_tweets[tweet_obj['id']]['attr_retweet_times'] = tweet_obj['attr_retweet_times']
									combined_tweets[tweet_obj['id']]['attr_retweet_times'] = list(set(combined_tweets[tweet_obj['id']]['attr_retweet_times']))
								
								if 'retweeters' in tweet_obj.keys():
									if 'retweeters' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['retweeters'] += tweet_obj['retweeters']
									else:
										combined_tweets[tweet_obj['id']]['retweeters'] = tweet_obj['retweeters']
									combined_tweets[tweet_obj['id']]['retweeters'] = list(set(combined_tweets[tweet_obj['id']]['retweeters']))
								
								if 'retweet_count' in tweet_obj.keys():
									if 'retweet_count' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['retweet_count'] = max(combined_tweets[tweet_obj['id']]['retweet_count'],tweet_obj['retweet_count'])
									else:
										combined_tweets[tweet_obj['id']]['retweet_count'] = tweet_obj['retweet_count']

								if 'attr_replies_tweets' in tweet_obj.keys():
									if 'attr_replies_tweets' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['attr_replies_tweets'] += tweet_obj['attr_replies_tweets']
									else:
										combined_tweets[tweet_obj['id']]['attr_replies_tweets'] = tweet_obj['attr_replies_tweets']
									combined_tweets[tweet_obj['id']]['attr_replies_tweets'] = list(set(combined_tweets[tweet_obj['id']]['attr_replies_tweets']))
								
								if 'attr_replies_times' in tweet_obj.keys():
									if 'attr_replies_times' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['attr_replies_times'] += tweet_obj['attr_replies_times']
									else:
										combined_tweets[tweet_obj['id']]['attr_replies_times'] = tweet_obj['attr_replies_times']
									combined_tweets[tweet_obj['id']]['attr_replies_times'] = list(set(combined_tweets[tweet_obj['id']]['attr_replies_times']))

								if 'reply_count' in tweet_obj.keys():
									if 'reply_count' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['reply_count'] = max(combined_tweets[tweet_obj['id']]['reply_count'],tweet_obj['reply_count'])
									else:
										combined_tweets[tweet_obj['id']]['reply_count'] = tweet_obj['reply_count']
								
								if 'favorite_count' in tweet_obj.keys():
									if 'favorite_count' in combined_tweets[tweet_obj['id']].keys():
										combined_tweets[tweet_obj['id']]['favorite_count'] = max(combined_tweets[tweet_obj['id']]['favorite_count'],tweet_obj['favorite_count'])
									else:
										combined_tweets[tweet_obj['id']]['favorite_count'] = tweet_obj['favorite_count']

				results = writeToSolr(combined_tweets, core, file)
				
			except Exception as exp:
				handleException(exp, 'Unknown', 'import_data_to_solr -> __main__')
			
			try:
				if results:
					try:
						if limit not in file:
							with open(file, 'rb') as f_in:
								with gzip.open(f"{file}.gz", 'ab') as f_out:
									shutil.copyfileobj(f_in, f_out)
								os.remove(file)
					except Exception as exp1:
						handleException(exp1, file, '__main__ -> Finally')
			except Exception as exp:
				handleException(exp, 'Unknown', 'import_data_to_solr -> __main__')
			