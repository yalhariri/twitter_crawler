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
config_path = '../.config/.configs.yml'
try:
    with open(config_path) as file:
        configs = yaml.load(file, Loader=yaml.FullLoader)
        PROJECT= configs['PROJECT']
        KEY = configs['KEY']
except Exception as exp:
    print(exp)
    print(f'Please make sure that the file {config_path} has the required information')


if __name__== "__main__":
	run = True
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('-s', '--start', help="please specify start point", default=None)
	parser.add_argument('-c', '--core', help="please specify core", default=None)
	parser.add_argument('-u', '--username', help="Solr username", default=None)
	parser.add_argument('-p', '--password', help="Solr username", default=None)
	
	args = parser.parse_args()
	core = args.core
	uname = args.username
	pswrd = args.password
	
	if uname == None:
		uname = input('Please enter solr username: ')
	if pswrd == None:
		pswrd = getpass.getpass(prompt='solr password: ', stream=None)
	
	if core != None and uname != None and pswrd != None:
		atexit.register(exitHandler, project =core, tool='Location Updater', key=KEY)

		confirmALife(core,tool=f':: {core} - Location Updater ::', key=KEY)
		
		url = 'http://127.0.0.1:10099/solr/'+ str(core) + '/'		
		solr = pysolr.Solr(url, auth=(uname,pswrd),timeout=120)
		start = 0
		if args.start != None:
			start = int(args.start)
		rows = 10000

		result = solr.search(q='NOT emotion:location_done AND NOT (location_gps:not_available AND user_location_original:not_available)', **{'fl':'id, place_country, user_location_original, place_full_name','start':start , 'rows':rows})
		max_row = result.hits
		start = max_row
		with open (status_file, 'w') as fout:
			fout.write('1')
		while start > 0 and run:
			start = max(start - rows, 0)
			result = solr.search(q='NOT emotion:location_done AND NOT (location_gps:not_available AND user_location_original:not_available)', **{'fl':'id, place_country, user_location_original, place_full_name','start':start , 'rows':rows})
			max_row = result.hits
			
			print('START : ',start)
			print('max_row : ',max_row)
			print('number of hits : ',max_row)

			tweets_all = list(result.docs.copy())
			tweets_list = []
			while len(tweets_all) > 0:
				tweets = tweets_all[0:1000]
				tweets_all = tweets_all[1000:]
				loc_dict = dict()
				for tweet in tweets:
					loc_dict[tweet['id']] = {'id': tweet['id'], 'user': {'location': tweet['user_location_original'] if 'user_location_original' in tweet.keys() else 'not_available'}, 'place': {'country': tweet['place_country'] if 'place_country' in tweet.keys() else 'not_available', 'full_name': tweet['place_full_name'] if 'place_full_name' in tweet.keys() else 'not_available'}}
				
				locations = get_location(loc_dict)
				
				if locations != None:
					for k in locations.keys():
						tweets_list.append({'id': k, 'user_location': locations[k]['user'], 'location_gps': locations[k]['tweet'], 'emotion': 'location_done'})
				if len(tweets_list) >= 5000:
					if write_location_to_solr(tweets_list, solr, max_row):					
						tweets_list = []
			
			if len(tweets_list) > 0:
				if write_location_to_solr(tweets_list, solr, max_row):
					tweets_list = []

			with open(status_file,'r') as f_in:
				kRun = f_in.readline().strip()
			if kRun == '0':
				print('Exitting as requested, please wait for pending process...' , end='')
				run = False
	else:
		print('Please make sure that command contains username, password and core.')
