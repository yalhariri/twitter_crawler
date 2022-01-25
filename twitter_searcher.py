#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 12:00:00 2021

@author: Youssef Al Hariri mailto:yalhariri@outlook.com

Searcher for Twitter API 2

It stores the Tweets contents into file (tweets), tweets from includes object (includes) and users object (users).

To combine the tweets contents with their relevant objects, use function extract_tweets_contents()
"""
import pandas as pd
import json
import os
from os import path, listdir, remove
from os.path import exists, join
import sys
import requests
from datetime import datetime
import time
import yaml
import logging
import logging.handlers

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response

def write_data_to_file(tweets, file_name, folder):
    """A function to write data into a file

    Args:
        tweets (dict): the dictionary of the tweets with their extracted information.
        file_name (str): the file name in which the data will be writen to.
        folder (str): the folder in which the file will be written to.
    """
    if not path.exists(folder):
        os.mkdir(folder)
    with open(folder+'/'+file_name,'a+',encoding='utf-8') as fout:
        for tweet in tweets:
            fout.write('%s\n'%json.dumps(tweet, ensure_ascii=False))

def get_json_content(file_name):
    temp_dict = dict()
    print(file_name)
    with open(OUTPUT_FOLDER +'/'+ file_name, 'r', encoding="utf-8") as fin:
        for line in fin.readlines():
            object_ = json.loads(line)
            temp_dict[object_['id']] = object_
    return temp_dict

def extract_tweets_contents(includes_file='', users_file='', tweets_file='',type=''):
    print('here')
    includes_dict = get_json_content(includes_file)
    users_dict = get_json_content(users_file)
    tweets_dict = get_json_content(tweets_file)
    
    combined_tweets_dict2 = dict()

    for k in includes_dict.keys():
        combined_tweets_dict2[k] = includes_dict[k]
        if includes_dict[k]['author_id'] in users_dict.keys():
            combined_tweets_dict2[k]['user'] = users_dict[includes_dict[k]['author_id']]
            
    for k in tweets_dict.keys():
        if 'referenced_tweets' in tweets_dict[k].keys():
            for referenced_tweet in tweets_dict[k]['referenced_tweets']:
                if 'referenced' not in combined_tweets_dict2.keys():
                    combined_tweets_dict2['referenced'] = [k]
                else:
                    if k not in combined_tweets_dict2['referenced']:
                        combined_tweets_dict2['referenced'].append(k)
                if referenced_tweet['type'] == 'retweeted':
                    if referenced_tweet['id'] in combined_tweets_dict2.keys():
                        if tweets_dict[k]['author_id'] in users_dict.keys():
                            if 'retweeters' not in combined_tweets_dict2[referenced_tweet['id']].keys():
                                combined_tweets_dict2[referenced_tweet['id']]['retweeters'] = [tweets_dict[k]['author_id']]
                            else:
                                #if tweets_dict[k]['author_id'] not in combined_tweets_dict2[referenced_tweet['id']]['retweeters']:
                                    combined_tweets_dict2[referenced_tweet['id']]['retweeters'].append(tweets_dict[k]['author_id'])
                            if 'retweets' not in combined_tweets_dict2[referenced_tweet['id']].keys():
                                combined_tweets_dict2[referenced_tweet['id']]['retweets'] = [k]
                            else:
                                if k not in combined_tweets_dict2[referenced_tweet['id']]['retweets']:
                                    combined_tweets_dict2[referenced_tweet['id']]['retweets'].append(k)
                elif referenced_tweet['type'] == 'replied_to':
                    combined_tweets_dict2[k] = tweets_dict[k]
                    if referenced_tweet['id'] in combined_tweets_dict2.keys():
                        if tweets_dict[k]['author_id'] in users_dict.keys():
                            if 'replied_to' not in combined_tweets_dict2[referenced_tweet['id']].keys():
                                combined_tweets_dict2[referenced_tweet['id']]['replied_to'] = [tweets_dict[k]['author_id']]
                            else:        
                                #if tweets_dict[k]['author_id'] not in combined_tweets_dict2[referenced_tweet['id']]['replied_to']:
                                    combined_tweets_dict2[referenced_tweet['id']]['replied_to'].append(tweets_dict[k]['author_id'])
                            if 'replies' not in combined_tweets_dict2[referenced_tweet['id']].keys():
                                combined_tweets_dict2[referenced_tweet['id']]['replies'] = [k]
                            else:        
                                if k not in combined_tweets_dict2[referenced_tweet['id']]['replies']:
                                    combined_tweets_dict2[referenced_tweet['id']]['replies'].append(k)
                elif referenced_tweet['type'] == 'quoted':
                    combined_tweets_dict2[k] = tweets_dict[k]
                    if referenced_tweet['id'] in combined_tweets_dict2.keys():
                        if tweets_dict[k]['author_id'] in users_dict.keys():
                            if 'quoters' not in combined_tweets_dict2[referenced_tweet['id']].keys():
                                combined_tweets_dict2[referenced_tweet['id']]['quoters'] = [tweets_dict[k]['author_id']]
                            else:
                                #if tweets_dict[k]['author_id'] not in combined_tweets_dict2[referenced_tweet['id']]['quoters']:
                                    combined_tweets_dict2[referenced_tweet['id']]['quoters'].append(tweets_dict[k]['author_id'])
                            if 'quotes' not in combined_tweets_dict2[referenced_tweet['id']].keys():
                                combined_tweets_dict2[referenced_tweet['id']]['quotes'] = [k]
                            else:
                                if k not in combined_tweets_dict2[referenced_tweet['id']]['quotes']:
                                    combined_tweets_dict2[referenced_tweet['id']]['quotes'].append(k)
                else:
                    print(referenced_tweet)
        else:
            combined_tweets_dict2[k] = tweets_dict[k]
            if tweets_dict[k]['author_id'] in users_dict.keys():
                combined_tweets_dict2[k]['user'] = users_dict[tweets_dict[k]['author_id']]
                
    if type == 'csv':
        temp_dict = dict()
        print(combined_tweets_dict2['949049212653600768'])
        for k in combined_tweets_dict2.keys():
            if k != 'referenced':            
                temp_dict[k] = {'id':combined_tweets_dict2[k]['id'],'created_at':combined_tweets_dict2[k]['created_at'],'full_text':combined_tweets_dict2[k]['text']}
                if 'user' in combined_tweets_dict2[k].keys():
                    if 'username' in combined_tweets_dict2[k]['user'].keys():
                        temp_dict[k]['screen_name'] = combined_tweets_dict2[k]['user']['username']
                    elif 'screen_name' in combined_tweets_dict2[k]['user'].keys():
                        temp_dict[k]['screen_name'] = combined_tweets_dict2[k]['user']['screen_name']
                if 'entities' in combined_tweets_dict2[k].keys():
                    if 'hashtags' in combined_tweets_dict2[k]['entities'].keys():
                        temp_dict[k]['hashtags'] = []
                        for x in combined_tweets_dict2[k]['entities']['hashtags']:
                             if 'text' in x.keys():
                                temp_dict[k]['hashtags'].append(x['text'])
                             elif 'tag' in x.keys():
                                temp_dict[k]['hashtags'].append(x['tag'])
                            
        try:
            df = pd.DataFrame.from_dict(temp_dict, orient='index')
            print(df)
            df.to_csv(OUTPUT_FOLDER+'/combined.csv', encoding='utf-8', index=False)
        except Exception as exp:
            print(exp)
    else:
        with open(OUTPUT_FOLDER+'/combined.json','w',encoding='utf-8') as fout:
            for k in combined_tweets_dict2.keys():
                fout.write('%s\n'%json.dumps(combined_tweets_dict2[k],ensure_ascii=False))
                

def search_for_tokens(headers):
    search_url = "https://api.twitter.com/2/tweets/search/all"
    
    days = [1, 10,20]
    months = [x for x in range(1,13)]
    years = list(range(2018,2023))
    dates = []
    start_date = datetime(START_DATE[0],START_DATE[1],START_DATE[2],0,0,0)
    end_date = datetime(END_DATE[0],END_DATE[1],END_DATE[2],0,0,0)

    for year in years:
        for month in months:
            for day in days:
                da = datetime(year,month,day,0,0,0) 
                if da > end_date:
                    break
                if da >= start_date and da <= end_date:
                    dates.append(datetime.strftime(da, '%Y-%m-%dT%H:%M:%SZ'))#datetime.strftime(da,'YYYY-MM-DDThh:mm:ssZ'))
                
    done_dates = []
    try:
        with open('done','r') as fin:
            done_dates = fin.readlines()
    except Exception as exp:
        pass
    

    print(QUERY)

    print("starting after {} sec".format(WAIT_TIME))
    time.sleep(WAIT_TIME)
    
    print(len(dates))
    for i in range(0,len(dates)-1):
        print('i:' , str(i))
        if dates[i] not in done_dates:
            query_params = {'query': ' OR '.join(QUERY),
                            'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld', 
                            'expansions' : 'author_id,referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id',
                            'user.fields': 'description,location,protected,verified,url,public_metrics,created_at,name,username,id',
                            'max_results': 100,
                            'start_time': dates[i], 
                            'end_time': dates[i+1]}
            print(query_params['start_time'] , ' TO ', query_params['end_time'])
            
            next_token = 'b26v89c19zqg8o3fn0gl5fulrau83vt54qqh4npg3t6d9'
            while (next_token):
                print('next_token:' , str(next_token))
                response = None
                try:
                    response = connect_to_endpoint(search_url, headers, query_params)
                except Exception as exp:
                    print(exp)
                    if exp.args[0] == 429:
                        print('Too many requests!')
                        print('Waiting for a preriod of time')
                        time.sleep(120)
                        response = None
                if response != None:
                    json_response = response.json()
                    tweets = None
                    users = None
                    includes = None
                    if 'data' in json_response.keys():
                        tweets = json_response['data']
                    if 'includes' in json_response.keys():
                        if 'users' in json_response['includes'].keys():
                            users = json_response['includes']['users']
                        if 'tweets' in json_response['includes'].keys():
                            includes = json_response['includes']['tweets']
                    meta = json_response['meta']
                    
                    filename = "{} TO {}".format(dates[i].replace('T00:00:00Z',''),dates[i+1].replace('T00:00:00Z',''))
                    if tweets != None:
                        write_data_to_file(tweets, 'tweets_'+filename, OUTPUT_FOLDER)
                    if users != None:
                        write_data_to_file(users, 'users_'+filename, OUTPUT_FOLDER)
                    if includes != None:
                        write_data_to_file(includes, 'includes_'+filename, OUTPUT_FOLDER)

                    if 'next_token' in meta.keys():
                        next_token = meta['next_token']
                        query_params['next_token'] = next_token
                    else:
                        next_token = False
            with open('done','a+') as fout:
                fout.write('{}\n'.format(dates[i]))

if __name__=="__main__":

    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--config', help="configuration setting file.", default='.config.yml')
    parser.add_argument('-cmd','--command', help="commands: search, extract_info", default=None)
    parser.add_argument('-tw','--tweets', help="tweets file inside the OTPUT_FOLDER", default='')
    parser.add_argument('-us','--users', help="users file inside the OTPUT_FOLDER", default='')
    parser.add_argument('-in','--includes', help="includes file inside the OTPUT_FOLDER", default='')
    parser.add_argument('-ty','--type', help="type of data, json or csv", default='csv')
    
    args = parser.parse_args()
    
    try:
        with open(r'../.config/'+str(args.config)) as file:
            configs = yaml.load(file, Loader=yaml.FullLoader)
            BEARER_TOKEN = configs['BEARER_TOKEN']
            OUTPUT_FOLDER = configs['OUTPUT_FOLDER']
            START_DATE= [int(v) for v in configs['START_DATE'].split('/')]
            END_DATE= [int(v) for v in configs['END_DATE'].split('/')]
            WAIT_TIME = int(configs['WAIT_TIME'])
            QUERY= configs['QUERY'].split(',')
            LOG= configs['LOG']
    except Exception as exp:
        print(exp)
        print('Please make sure that config/config.yml has the required information')
        BEARER_TOKEN=""
    if (not os.path.exists(LOG)):
        os.makedirs(LOG)
        
    if not args.command:
        sys.exit('ERROR: COMMAND is required')
        
    
    formatter = logging.Formatter('(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')
    handler = logging.handlers.RotatingFileHandler(
        '%s/%s.log'%(LOG,args.command), maxBytes=50 * 1024 * 1024, backupCount=10)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info(sys.version)
    
    
    if BEARER_TOKEN != "":
        headers = create_headers(BEARER_TOKEN)
        if args.command == 'search':
            search_for_tokens(headers)
        elif args.command == 'extract_info':
            if args.tweets != '' and args.users != '' and args.includes != '':
                extract_tweets_contents(args.includes, args.users, args.tweets, args.type)
        else:
            print('Command not found!')
    