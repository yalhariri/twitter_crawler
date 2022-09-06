# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 20:23:04 2021

@author: yalhariri@outlook.com
"""

status_file = '../.logs/status_file_locations'
import pysolr
from util import *
import getpass
import yaml
import atexit
import tarfile
import os
from os import listdir
from datetime import datetime
from os.path import isfile, join
			
config_path = '../.config/.configs.yml'

try:
    with open(config_path) as file:
        configs = yaml.load(file, Loader=yaml.FullLoader)
        PROJECT= configs['PROJECT']
        KEY = configs['KEY']
except Exception as exp:
    print(exp)
    print(f'Please make sure that the file {config_path} has the required information')

def remove_new_lines(text):
	return re.sub("[\n]+" , ". ", text)

if __name__== "__main__":
	run = True
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('-s', '--start', help="please specify start point", default=None)
	parser.add_argument('-c', '--core', help="please specify core", default=None)
	parser.add_argument('-u', '--username', help="Solr username", default=None)
	parser.add_argument('-p', '--password', help="Solr username", default=None)
	parser.add_argument('-tf', '--tweets_file', help="Solr username", default=None)
	

	args = parser.parse_args()
	core = args.core
	uname = args.username
	pswrd = args.password
	tweets_file = args.tweets_file
	new_tweets_file = tweets_file.replace(".csv", f"_done_{datetime.strftime(datetime.now(),'%Y_%d')}.tar.gz")

	print(f"tweets_file: {tweets_file}")

	if uname == None:
		uname = input('Please enter solr username: ')
	if pswrd == None:
		pswrd = getpass.getpass(prompt='solr password: ', stream=None)
	
	if core != None and uname != None and pswrd != None:
		atexit.register(exitHandler, project =core, tool='Aspect Updater', key=KEY)
		confirmALife(core,tool=f':: {core} - Aspect Updater ::', key=KEY)
		
		with open (status_file, 'w') as fout:
			fout.write('1')
		
		url = 'http://127.0.0.1:10099/solr/'+ str(core) + '/'		
		solr = pysolr.Solr(url, auth=(uname,pswrd),timeout=120)
		q='NOT aspect1_s:* OR aspect1_s:none'
		result = solr.search(q=q, **{'fl':'id'})
		
		rows = 10000
		start = result.hits
		tweets_list = []
		while start > 0:
			start = max(0, start - rows)
			result = solr.search(q=q, **{'fl':'id, full_text, language','start':start , 'rows':rows})
			tweets_list = tweets_list + list(result.docs)
		
		rec_df = pd.DataFrame.from_records(tweets_list)
		
		print(f"len(rec_df['language'] == 'english'): {len(rec_df[rec_df['language'] == 'english'])}")
		print(f"len(rec_df['language'] != 'english'): {len(rec_df[rec_df['language'] != 'english'])}")
		if len(rec_df[rec_df['language'] == 'english']) > 100 and len(rec_df[rec_df['language'] != 'english']) > 100:
			rec_df['text'] = rec_df.full_text.apply(remove_new_lines)
			rec_df = rec_df[['id','text','language']]

			if os.path.exists(tweets_file):
				if not os.path.exists(new_tweets_file):
					with tarfile.open(new_tweets_file, "w:gz") as tar:
						tar.add(tweets_file, arcname=tweets_file.split('/')[-1])
				rec_df.to_csv(tweets_file, index=False)
			else:
				rec_df.to_csv(tweets_file, index=False)
		else:
			print('Number of element is less than 100. No tweets file created!')
	else:
		print('Please make sure that command contains username, password and core.')