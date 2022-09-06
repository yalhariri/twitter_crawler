# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 20:23:04 2021

@author: yalhariri@outlook.com
"""

status_file = '../.logs/status_file_locations'
from ast import Lambda
import pysolr
from util import *
import getpass
import yaml
import atexit
import tarfile
import os
import numpy as np
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
	parser.add_argument('-af', '--aspect_file', help="Solr username", default=None)
	
	args = parser.parse_args()
	core = args.core
	uname = args.username
	pswrd = args.password
	
	aspect_file = args.aspect_file
	new_aspect_file = aspect_file.replace(".csv", f"_done_{datetime.strftime(datetime.now(),'%Y_%d')}.tar.gz")

	print(f"aspect_file: {aspect_file}")

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
		
		if os.path.exists(aspect_file):
			aspects_df = pd.read_csv(aspect_file)

			if len(aspects_df) > 0:
				aspects_df = aspects_df.rename(columns={'aspect1':'aspect1_s' , 'aspect2':'aspect2_s'})
				#aspects_df = aspects_df.where(pd.notnull(aspects_df), "")
				#aspects_df = aspects_df.loc[np.where((aspects_df['aspect1_s']!= "") | (aspects_df['aspect2_s']!= ""))]
				aspects_dict = aspects_df[['id','aspect1_s','aspect2_s']].to_dict(orient='index')
				max_row = len(aspects_dict)
				aspects_list = list(aspects_dict.values())
				for item in aspects_list:
					item['id'] = str(item['id'])
				while len(aspects_list) > 0:
					tweets_list = aspects_list[0:10000]
					if write_aspects_to_solr(tweets_list, solr, max_row):					
						aspects_list = aspects_list[10000:]
						tweets_list = []
					
				if len(tweets_list) > 0:
					if write_aspects_to_solr(tweets_list, solr, max_row):
						tweets_list = []
				
				compressed = False
				if not os.path.exists(new_aspect_file):
					with tarfile.open(new_aspect_file, "w:gz") as tar:
						tar.add(aspect_file, arcname=aspect_file.split('/')[-1])
						compressed = True
					if compressed:
						if os.path.exists(aspect_file):
							if isfile(aspect_file):
								os.remove(aspect_file)

			with open(status_file,'r') as f_in:
				kRun = f_in.readline().strip()
			if kRun == '0':
				print('Exitting as requested, please wait for pending process...' , end='')
				run = False
		else:
			print('No aspect file detected... No aspect updated on Solr!')
	else:
		print('Please make sure that command contains username, password and core.')