#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 12:00:00 2021
@author: Youssef Al Hariri mailto:yalhariri@outlook.com
Searcher for Twitter API 2
It stores the Tweets contents into file (tweets), tweets from includes object (includes) and users object (users).
To combine the tweets contents with their relevant objects, use function extractTweetsContents()
"""
import pandas as pd
import json
import os
from os import path, listdir, remove
from os.path import exists, join
import sys
import requests
from datetime import datetime, timedelta
import time
import yaml
import logging
import logging.handlers
from util import *
import atexit

status_file = "../.cache/status_searcher"
if not path.exists('../.cache'):
        os.mkdir('../.cache')


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')

def create_headers1(bearer_token1, bearer_token2=None):
    headers1 = {"Authorization": "Bearer {}".format(bearer_token1)}
    headers = [headers1] 
    if bearer_token2 != None:
        headers2 = {"Authorization": "Bearer {}".format(bearer_token2)}
        headers = [headers1 , headers2]
    return headers

def create_headers(bearer_tokens=[]):
    headers = [] 
    for bearer_token in bearer_tokens:
        headers.append({"Authorization": "Bearer {}".format(bearer_token)})
    return headers


def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response

def extractTweetsContents(OUTPUT_FOLDER, includes_file='', users_file='', tweets_file='',type=''):
    print('here')
    includes_dict = getJSONContent(includes_file)
    users_dict = getJSONContent(users_file)
    tweets_dict = getJSONContent(tweets_file)
    
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
                

def search_for_tokens(headers, dates, next_token):
    search_url = "https://api.twitter.com/2/tweets/search/recent"
    
    
    done_dates = []
    try:
        with open('done','r') as fin:
            done_dates = fin.readlines()
    except Exception as exp:
        pass
    print(f'dates: {dates}\nstart_date: {dates[0]}\nend_date: {dates[-1]}\n\nheaders: {headers}\n')
    print(f'QUERY: {QUERY_list}')
    print("starting after {} sec".format(WAIT_TIME))
    time.sleep(WAIT_TIME)
    print(len(dates))
    turn = 0
    for i in range(0,len(dates)-1):
        print('i:' , str(i))
        if dates[i] not in done_dates:
            query_params = {'query': ' OR '.join(QUERY_list),
                            'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld', 
                            'expansions' : 'author_id,referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id',
                            'user.fields': 'description,location,protected,verified,url,public_metrics,created_at,name,username,id,entities,pinned_tweet_id,profile_image_url,withheld',
                            'media.fields': 'duration_ms,height,media_key,preview_image_url,public_metrics,type,url,width,alt_text,variants',
                            'place.fields':'contained_within,country,country_code,full_name,geo,id,name,place_type',
                            'max_results': 100,
                            'start_time': dates[i], 
                            'end_time': dates[i+1]}
            print(query_params['start_time'] , ' TO ', query_params['end_time'])
            next_token = True
            while (next_token):
                if next_token != True:
                    query_params['next_token'] = next_token
                response = None
                try:
                    response = connect_to_endpoint(search_url, headers[turn], query_params)
                except Exception as exp:
                    logger.error(exp)
                    if exp.args[0] == 429:
                        print('Too many requests!')
                        print('Waiting for a preriod of time')
                        turn =(turn+1)%len(headers)
                        time.sleep(30)
                        response = None
                    else:
                        print(exp)
                if response != None:
                    json_response = response.json()
                    outputFile = f"{dates[i].replace('T00:00:00Z','')}"
                    outputFile = outputFile.replace('-','_')
                    filename = f'RAW_SEARCH_{outputFile}.json'
                    try:
                        writeDataToFile(json_response, filename, OUTPUT_FOLDER)
                    except Exception as exp:
                        handleException(exp, object_=json_response, func_=f'\n{__name__}')
                    try:
                        users = None
                        includes = None
                        places = None
                        media = None
                        poll = None
                        tweets = json_response['data'] if 'data' in json_response.keys() else None
                        if 'includes' in json_response.keys():
                            users = json_response['includes']['users'] if 'users' in json_response['includes'].keys() else None
                            includes = json_response['includes']['tweets'] if 'tweets' in json_response['includes'].keys() else None
                            places = json_response['includes']['places'] if 'places' in json_response['includes'].keys() else None
                            media = json_response['includes']['media'] if 'media' in json_response['includes'].keys() else None
                            poll = json_response['includes']['poll'] if 'poll' in json_response['includes'].keys() else None
                        
                        retweets_dict = dict()
                        replies_dict = dict()
                        quotes_dict = dict()
                        tweets_dict = dict()
                        users_dict = dict()
                        media_dict = dict()
                        places_dict = dict()
                                    
                        places_dict = extractResponseContentsFromDict(places, places_dict)
                        media_dict = extractMediaContentsFromDict(media, media_dict)
                        users_dict = extractResponseContentsFromDict(users, users_dict)
                        tweets_dict, retweets_dict, replies_dict, quotes_dict = extractTweetsFromDict(includes, tweets_dict, False, users_dict, places_dict, retweets_dict, replies_dict, quotes_dict, media_dict)
                        tweets_dict, retweets_dict, replies_dict, quotes_dict = extractTweetsFromDict(tweets, tweets_dict, True, users_dict, places_dict, retweets_dict, replies_dict, quotes_dict, media_dict)
                        print(f'Tweets : {len(tweets)} and tweets_dict: {len(tweets_dict)} and retweets_dict: {len(retweets_dict)}')

                        combined_dict = tweets_dict.copy()

                        for k in replies_dict.keys():
                            for j in replies_dict[k].keys():
                                if j not in combined_dict.keys():
                                    combined_dict[j] = replies_dict[k][j]
                        for k in quotes_dict.keys():
                            for j in quotes_dict[k].keys():
                                if j not in combined_dict.keys():
                                    combined_dict[j] = quotes_dict[k][j]
                        
                        for k in replies_dict.keys():
                            for j in replies_dict[k].keys():
                                if k in combined_dict.keys():
                                    if 'attr_replies_tweets' not in combined_dict[k].keys():
                                        combined_dict[k]['attr_replies_tweets'] = [j]
                                    else:
                                        if j not in combined_dict[k]['attr_replies_tweets']:
                                            combined_dict[k]['attr_replies_tweets'].append(j)

                                    attr_replies_times = f"{replies_dict[k][j]['user_screen_name']} {replies_dict[k][j]['created_at_time']}"
                                    if 'attr_replies_times' not in combined_dict[k].keys():
                                        combined_dict[k]['attr_replies_times'] = [attr_replies_times]
                                    if attr_replies_times not in combined_dict[k]['attr_replies_times']:
                                        combined_dict[k]['attr_replies_times'].append(attr_replies_times)
                            
                        for k in retweets_dict.keys():
                            print(k)
                            for j in retweets_dict[k].keys():
                                if k in combined_dict.keys():
                                    if 'retweeters' not in combined_dict[k].keys():
                                        combined_dict[k]['retweeters'] = [retweets_dict[k][j]['user_screen_name']]
                                    else:
                                        if retweets_dict[k][j]['user_screen_name'] not in combined_dict[k]['retweeters']:
                                            combined_dict[k]['retweeters'].append(retweets_dict[k][j]['user_screen_name'])
                                    if 'created_at' in retweets_dict[k][j].keys():
                                        attr_retweet_times = f"{retweets_dict[k][j]['user_screen_name']} {retweets_dict[k][j]['created_at']}"
                                    else:
                                        attr_retweet_times = f"{retweets_dict[k][j]['user_id']}"
                                        
                                    if 'attr_retweet_times' not in combined_dict[k].keys():
                                        combined_dict[k]['attr_retweet_times'] = [attr_retweet_times]
                                    if attr_retweet_times not in combined_dict[k]['attr_retweet_times']:
                                        combined_dict[k]['attr_retweet_times'].append(attr_retweet_times)
                        
                        for k in quotes_dict.keys():
                            for j in quotes_dict[k].keys():
                                if k in combined_dict.keys():
                                    if 'attr_quote_tweets' not in combined_dict[k].keys():
                                        combined_dict[k]['attr_quote_tweets'] = [j]
                                    else:
                                        if j not in combined_dict[k]['attr_quote_tweets']:
                                            combined_dict[k]['attr_quote_tweets'].append(j)

                                    if 'attr_quoters' not in combined_dict[k].keys():
                                        combined_dict[k]['attr_quoters'] = [quotes_dict[k][j]['user_screen_name']]
                                    else:
                                        if quotes_dict[k][j]['user_screen_name'] not in combined_dict[k]['attr_quoters']:
                                            combined_dict[k]['attr_quoters'].append(quotes_dict[k][j]['user_screen_name'])
                                    if 'created_at' in quotes_dict[k][j].keys():
                                        attr_quote_times = f"{quotes_dict[k][j]['user_screen_name']} {quotes_dict[k][j]['created_at']}"
                                    else:
                                        attr_quote_times = f"{quotes_dict[k][j]['user_id']}"
                                        
                                    if 'attr_quote_times' not in combined_dict[k].keys():
                                        combined_dict[k]['attr_quote_times'] = [attr_quote_times]
                                    
                                    if attr_quote_times not in combined_dict[k]['attr_quote_times']:
                                        combined_dict[k]['attr_quote_times'].append(attr_quote_times)
                        
                        with open(f'{OUTPUT_FOLDER_PROCESSED}/{outputFile}', 'a+', encoding='utf-8') as fout:
                            for k in combined_dict.keys():
                                fout.write(f"{json.dumps(combined_dict[k], ensure_ascii=False)}\n")
                        
                        

                        if 'meta' in json_response.keys():
                            next_token = json_response['meta']['next_token'] if 'next_token' in json_response['meta'].keys() else False
                            
                                
                    except Exception as exp:
                        handleException(exp, 'Unknown', __name__)

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
    parser.add_argument('-tr','--terms', help="terms file", default='')
    parser.add_argument('-ty','--type', help="type of data, json or csv", default='csv')
    
    args = parser.parse_args()
    
    try:
        with open(str(args.config)) as file:
            configs = yaml.load(file, Loader=yaml.FullLoader)
            BEARER_TOKEN = configs['BEARER_TOKENS']
            OUTPUT_FOLDER = configs['OUTPUT_FOLDER']
            OUTPUT_FOLDER_PROCESSED = configs['OUTPUT_FOLDER'].replace('streaming', 'processed')
            START_DATE= [int(v) for v in configs['START_DATE'].split('/')]
            END_DATE= [int(v) for v in configs['END_DATE'].split('/')]
            WAIT_TIME = int(configs['WAIT_TIME'])
            QUERY= configs['QUERY']
            LOG= configs['LOG']
            PROJECT= configs['PROJECT']
            KEY = configs['KEY']
            next_token= configs['next_token']
    except Exception as exp:
        print(exp)
        print('Please make sure that config/config.yml has the required information')
        BEARER_TOKEN=[]
        handleException(args.command, exp)
    if (not os.path.exists(LOG)):
        os.makedirs(LOG)
        
    if not args.command:
        sys.exit('ERROR: COMMAND is required')
    
    print(args.terms)
    with open (f'../.config/{args.terms}', 'r', encoding="utf-8") as fin:
        QUERY_list = [x.strip() for x in fin.readlines()]
    
    if (not os.path.exists(OUTPUT_FOLDER)):
        os.makedirs(OUTPUT_FOLDER)
    
    atexit.register(exitHandler, project =PROJECT, tool = 'Twitter Searcher', key=KEY)

    formatter = logging.Formatter('(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')
    handler = logging.handlers.RotatingFileHandler(
        '%s/%s.log'%(LOG,args.command), maxBytes=50 * 1024 * 1024, backupCount=10)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info(sys.version)
    
    headers = create_headers(BEARER_TOKEN)
    if len(headers) > 0:
        if args.command == 'search':
            days = [1, 10,20]
            months = [x for x in range(1,13)]
            years = list(range(2018,2023))
            dates = []
            start_date = datetime(START_DATE[0],START_DATE[1],START_DATE[2],0,0,0)
            end_date = datetime(END_DATE[0],END_DATE[1],END_DATE[2],0,0,0)

            for year in years:
                for month in months:
                    for day in range(1,32):
                        try:
                            da = datetime(year,month,day,0,0,0) 
                            if da > end_date:
                                break
                            if da >= start_date and da <= end_date:
                                dates.append(datetime.strftime(da, '%Y-%m-%dT%H:%M:%SZ'))
                        except Exception as exp:
                            pass
            if len(dates)>0:
                search_for_tokens(headers, dates, next_token)
        if args.command == 'daily_search':
            dates = [(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z"), datetime.today().strftime("%Y-%m-%dT00:00:00Z")]
            search_for_tokens(headers, dates,next_token)
        elif args.command == 'get_timelines':
            search_for_tokens(headers, next_token)
        elif args.command == 'extract_info':
            if args.tweets != '' and args.users != '' and args.includes != '':
                extractTweetsContents(OUTPUT_FOLDER, args.includes, args.users, args.tweets, args.type)
        else:
            print('Command not found!')
    