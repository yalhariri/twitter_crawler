# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 20:23:04 2021

@author: yalhariri@outlook.com
"""
import pysolr
import time
import yaml
import atexit
from datetime import datetime
from util import *

config_path = '../.config/.configs.yml'
try:
    with open(config_path) as file:
        configs = yaml.load(file, Loader=yaml.FullLoader)
        PROJECT= configs['PROJECT']
        KEY = configs['KEY']
except Exception as exp:
    print(exp)
    print(f'Please make sure that the file {config_path} has the required information')
	

status_file = 'status_file_sentiments_eng'
sleep_time = 3

if __name__== "__main__":
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('-s', '--start', help="please specify start point", default=None)
	parser.add_argument('-c', '--core', help="please specify core", default=None)
	parser.add_argument('-u', '--username', help="Solr username", default=None)
	parser.add_argument('-p', '--password', help="Solr username", default=None)
	run = True

	with open (status_file, 'w') as fout:
		fout.write('1')
	args = parser.parse_args()
	core = args.core
	uname = args.username
	pswrd = args.password

	if core != None and uname != None and pswrd != None :
		
		atexit.register(exitHandler, project =core, tool='Sentiment Updater', key=KEY)

		confirmALife(core,tool=f':: {core} - Sentiment Updater ::', key=KEY)

		url = 'http://127.0.0.1:10099/solr/'+ str(core) + '/'		
		solr = pysolr.Solr(url, auth=(uname,pswrd),timeout=120)
		start = 0
		retry=0
		if args.start != None:
			start = int(args.start)
		rows = 25000
		tweets_list = []
		sentiment = dict()
		q = 'NOT sentiment:* AND language:*'
		fl = 'id, full_text, language'
		result = solr.search(q=q, **{'fl':fl})
		max_rows = result.hits
		start = max_rows
		while run and  start > 0:
			start = max(start - rows, 0)
			result = solr.search(q=q, **{'fl':fl,'start':start , 'rows':rows})
			max_row = result.hits
			print(f'Solr query results gotten ... result.hits {result.hits}')
			
			now = datetime.now()
			print('Data loaded, start processing... Current Time =', now.strftime("%H:%M:%S"))
			
			for tweet in result.docs:
				try:
					if 'language' in tweet.keys():
						if tweet['language'] == 'english':
							sentiment[tweet["id"]] = {'id': tweet['id'], "full_text": tweet["full_text"], "language":'en'}
						else:
							language_ = tweet['language']
							temp_dict = {'id': tweet['id'], 'sentiment': 'NoneEnglish', 'language_s':language_}
							tweets_list.append(temp_dict)
					else:
						print(f'language not found in tweet! {tweet}')
				except Exception as exp:
					#print(f'Exception at update_sentiment line 69\n{exp}')
					handleException(exp,'Line 69',__name__)
				if len(sentiment) >= 4000:
					sentiments = None
					try:
						sentiments = get_sentiments(sentiment)
					except Exception as exp:
						#print(f'Exception at update_sentiment line 75\n{exp}')
						handleException(exp,'Line 75',__name__)
						time.sleep(1)
					if sentiments != None:
						print(list(sentiments)[0:5])
						sentiment = dict()
						for k in sentiments.keys():
							temp_dict = {'id': k, 'sentiment': sentiments[k], 'language_s':"english"}
							tweets_list.append(temp_dict)
				
				if len(tweets_list) >= 8000:
					if write_sentiment_to_solr(tweets_list ,solr, max_row, sleep_time):
						print('written to solr')
						tweets_list = []
					else:
						if sleep_time > 30:
							sleep_time = 3
						time.sleep(sleep_time)
			
			kRun = '1'
			with open(status_file,'r') as f_in:
				kRun = f_in.readline().strip()
			run = kRun == '1'
			
		if len(sentiment) >= 0:
			sentiments = None
			try:
				sentiments = get_sentiments(sentiment)
			except Exception as exp:
				#print(f'Exception at update_sentiment line 103\n{exp}')
				handleException(exp,'Line 106',__name__)
				time.sleep(1)
			finally:
				sentiment = dict()
			if sentiments != None:
				for k in sentiments.keys():
					temp_dict = {'id': k, 'sentiment': sentiments[k], 'language_s':"english"}
					tweets_list.append(temp_dict)
		
		if len(tweets_list) > 0:
			if write_sentiment_to_solr(tweets_list, solr, max_row, sleep_time):
				tweets_list = []
			else:
				print('Data not written to solr!')
	else:
		print('please enter core, username, password')
