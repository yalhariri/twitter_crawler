#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 12:00:00 2021
@author: Youssef Al Hariri mailto:yalhariri@outlook.com

Twitter API 2 Streamer 
It stores the Tweets contents into file (tweets), tweets from includes object (includes) and users object (users).
"""
from util import *
import json
import os
import sys
import time
import yaml
import atexit
import logging
import requests
import logging.handlers
from threading import Thread
from datetime import datetime

global BEARER_TOKEN
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')

status_file = "../.cache/status_streamer"
url = "https://api.twitter.com/2/tweets/search/stream"
if not path.exists('../.cache'):
        os.mkdir('../.cache')


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(f"{url}/rules", auth=bearer_oauth)
    if response.status_code != 200:
        raise Exception("Cannot get rules (HTTP {}): {}".format(response.status_code, response.text))
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None
    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(f"{url}/rules", auth=bearer_oauth, json=payload)
    if response.status_code != 200:
        raise Exception(f"Cannot delete rules (HTTP {response.status_code}): {response.text}.")
    print(json.dumps(response.json()))

def set_rules(rules):
    # You can adjust the rules if needed
    payload = {"add": rules}
    response = requests.post(f"{url}/rules", auth=bearer_oauth, json=payload,)
    if response.status_code != 201:
        raise Exception(f"Cannot add rules (HTTP {response.status_code}): {response.text}")    
    print(json.dumps(response.json()))


def startStreaming():
    confirmALife(PROJECT, 'Streamer',KEY)
    
    params = {'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld', 
              'expansions' : 'author_id,referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,attachments.poll_ids,attachments.media_keys,in_reply_to_user_id,geo.place_id',
              'user.fields': 'description,location,protected,verified,url,public_metrics,created_at,name,username,id,entities,pinned_tweet_id,profile_image_url,withheld',
              'place.fields':'contained_within,country,country_code,full_name,geo,id,name,place_type',
              'media.fields': 'duration_ms,height,media_key,preview_image_url,public_metrics,type,url,width,alt_text,variants',
              'poll.fields':'duration_minutes,end_datetime,id,options,voting_status'
              }
    response = requests.get(url, auth=bearer_oauth, stream=True, params=params)
    print(response.status_code)
    if response.status_code != 200:
        logger.warning(f"Cannot get stream (HTTP {response.status_code}): {response.text}")
        raise Exception(f"Cannot get stream (HTTP {response.status_code}): {response.text}")
    for response_line in response.iter_lines():
        if response_line:
            try:
                json_response = json.loads(response_line)
                filename = datetime.strftime(datetime.now(), "%Y_%m_%d_%H")
                writeDataToFile(json_response, f'RAW_DATA_{filename}', OUTPUT_FOLDER)
            except Exception as exp:
                handleException(exp, object_=json_response, func_=f'\n{__name__}')
    
if __name__=="__main__":

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--config', help="configuration setting file.", default='../.config/.config.yml')
    parser.add_argument('-cmd','--command', help="commands: search, extract_info", default=None)
    parser.add_argument('-tw','--tweets', help="tweets file inside the OTPUT_FOLDER", default='')
    parser.add_argument('-us','--users', help="users file inside the OTPUT_FOLDER", default='')
    parser.add_argument('-in','--includes', help="includes file inside the OTPUT_FOLDER", default='')
    parser.add_argument('-ty','--type', help="type of data, json or csv", default='csv')
    parser.add_argument('-sr','--set_rules', help="set streamer rules", default=True)
    
    args = parser.parse_args()
    
    try:
        with open(str(args.config)) as file:
            configs = yaml.load(file, Loader=yaml.FullLoader)
            BEARER_TOKENS = configs['BEARER_TOKENS']
            OUTPUT_FOLDER = configs['OUTPUT_FOLDER']
            PROJECT= configs['PROJECT']
            KEY = configs['KEY']
            LOG= configs['LOG']
    except Exception as exp:
        print(exp)
        print(f'Please make sure that the file {args.config} has the required information')
        BEARER_TOKENS=""
    if (not os.path.exists(LOG)):
        os.makedirs(LOG)
    
    if not args.command:
        sys.exit('ERROR: COMMAND is required')
    
    atexit.register(exitHandler, project =PROJECT, tool='Streamer', key=KEY)

    formatter = logging.Formatter('(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')
    handler = logging.handlers.RotatingFileHandler(f'{LOG}/{args.command}.log', maxBytes=50 * 1024 * 1024, backupCount=10)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info(sys.version)
    turn = 0

    if BEARER_TOKENS != "":
        kRun = writeRunningStatus(status_file,'1')
        message = 'Exitting as requested by status file'
        try:
            while(kRun):
                try:
                    kRun = readRunningStatus(status_file)
                    headers = create_headers(BEARER_TOKENS[turn])
                    if args.command == 'stream':
                        try:
                            BEARER_TOKEN = BEARER_TOKENS[turn]
                            logger.info('Streaming started with rules: {}.'.format(get_rules()))
                            startStreaming()
                        except Exception as exp:
                            handleException(exp, f'Error {exp}',__name__)
                        finally:
                            handleException('Finally...', 'Streaming stopped', __name__)
                            time.sleep(3)
                    else:
                        print('Command not found!')
                except Exception as exp:
                    handleException(exp, f'Error at {args.command}', __name__)
                    pass
        except KeyboardInterrupt as exp:
            message = 'Exitting as requested by user (KeyboardInterrupt)!'
            writeRunningStatus(status_file,'0')
            handleException(exp, f'{message}\n{args.command}', __name__)
            pass
        except Exception as exp:
            writeRunningStatus(status_file,'0')
            handleException(exp, f'Exitting due to an exception... at {args.command}', __name__)
            pass
        finally:
                kRun = readRunningStatus(status_file)
                if kRun:
                    print("restarting...!")
                    turn =(turn+1)%len(BEARER_TOKENS)
                    time.sleep(1)
                else:
                    print(message)
