#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 23 12:00:00 2021

@author: Youssef Al Hariri mailto:yalhariri@outlook.com

Searcher for Twitter API 2

It stores the Tweets contents into file (tweets), tweets from includes object (includes) and users object (users).

"""
import json
import os
import sys
from datetime import datetime
import time
import logging
import logging.handlers
import yaml
import atexit
from util import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')

    
def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None
    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(rules):
    # You can adjust the rules if needed

    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    
if __name__=="__main__":

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--config', help="configuration setting file.", default='../.config/.config.yml')
    parser.add_argument('-cmd','--command', help="commands: search, extract_info", default=None)
    
    args = parser.parse_args()
    
    try:
        with open(str(args.config)) as file:
            configs = yaml.load(file, Loader=yaml.FullLoader)
            BEARER_TOKENS = configs['BEARER_TOKENS']
            QUERY= configs['QUERY']
            LOG= configs['LOG']
    except Exception as exp:
        print(exp)
        print(f'Please make sure that the file {args.config} has the required information')
        BEARER_TOKENS=""
    if (not os.path.exists(LOG)):
        os.makedirs(LOG)
    
    print(QUERY)
    with open (QUERY, 'r', encoding="utf-8") as fin:
        QUERY_list = [x.strip() for x in fin.readlines()]

    with open(QUERY, 'r',encoding='utf-8') as stream:
        data_loaded = yaml.safe_load(stream)

    items_dict = dict()
    for line in data_loaded['GROUPS']:
        items = line.split(',')
        tag = ''
        tokens = []
        for item in items:
            if 'TAG_NAME' in item:
                tag = item.replace('TAG_NAME_','').strip()
            else:
                tokens.append(item.strip())
            items_dict[tag] = tokens
        
        items_dict
    print(f'=======\n{items_dict}\n-------')  

    formatter = logging.Formatter('(%(asctime)s) [%(process)d] %(levelname)s: %(message)s')
    handler = logging.handlers.RotatingFileHandler(
        '%s/%s.log'%(LOG,args.command), maxBytes=50 * 1024 * 1024, backupCount=10)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info(sys.version)
    turn = 0

    if BEARER_TOKENS != "":
        try:
            new_rules = []
            for k in items_dict.keys():
                str_ = ""
                for q in [x.strip() for x in items_dict[k]]:
                    if len(str_) == 0:
                        str_ = str(q)
                    elif len(str_) + len(q) >= 512:
                        print('ERROR: query length larger than limit...')
                        sys.exit(-1)
                    else:
                        str_ += ' OR {}'.format(str(q))
                new_rules.append({"value": str_, "tag": k})

            print(new_rules)

            for item in BEARER_TOKENS:
                BEARER_TOKEN = item
                rules = get_rules()
                if rules is not None:
                    delete = delete_all_rules(rules)
                set = set_rules(rules=new_rules)
                print(get_rules())
        except KeyboardInterrupt:
            print('Exitting as requested by (KeyboardInterrupt)!')
            update_log_excption(args.command, 'KeyboardInterrupt')
        except Exception as exc:
            print(exc)
            update_log_excption(args.command, exc)
            