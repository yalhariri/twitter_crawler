import os
import yaml
import json 
import shutil
import atexit
import tarfile
from os import listdir
from datetime import datetime
from os.path import isfile, join
from urllib.request import urlopen
from util import *

data_path = "../streaming"
OUTPUT_FOLDER = "../processed"
config_path = '../.config/.configs.yml'
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

try:
    with open(config_path) as file:
        configs = yaml.load(file, Loader=yaml.FullLoader)
        PROJECT= configs['PROJECT']
        KEY = configs['KEY']
except Exception as exp:
    print(exp)
    print(f'Please make sure that the file {config_path} has the required information')

atexit.register(exitHandler, project =PROJECT, tool='Extracting data', key=KEY)

if __name__== "__main__":
    confirmALife(PROJECT,tool='Extracting data', key=KEY)
    users_files = []
    day = datetime.now()
    limit = day.strftime('%Y_%m_%d') #day.strftime('%Y_%m_%d_%H')
    limit_day = day.strftime('%Y_%m_%d')
    
    work_files = list(set([join(data_path, f) for f in listdir(data_path) if limit not in f and isfile(join(data_path, f)) and not f.endswith('.gz') and f.startswith('RAW_')]))
    print(f'work_files: {work_files}')
    
    for workFile in sorted(work_files):
        print(workFile)
        retweets_dict = dict()
        replies_dict = dict()
        quotes_dict = dict()
        tweets_dict = dict()
        users_dict = dict()
        media_dict = dict()
        places_dict = dict()
        lines = 0
        tweets = []
        if os.path.exists(workFile):
            with open(workFile, 'r' , encoding='utf-8') as fin:
                for line in fin:
                    objects = json.loads(line.strip())
                    lines += 1
                    tweets, users, includes, places, media, poll = extract_raw_responses(objects)
                    places_dict = extractResponseContentsFromDict(places, places_dict)
                    media_dict = extractMediaContentsFromDict(media, media_dict)
                    users_dict = extractResponseContentsFromDict(users, users_dict)
                    if type(includes) == list:
                        for obj_ in includes:
                            tweets_dict, retweets_dict, replies_dict, quotes_dict = extractTweetsFromDict(obj_, tweets_dict, False, users_dict, places_dict, retweets_dict, replies_dict, quotes_dict, media_dict)
                    else:
                        tweets_dict, retweets_dict, replies_dict, quotes_dict = extractTweetsFromDict(includes, tweets_dict, False, users_dict, places_dict, retweets_dict, replies_dict, quotes_dict, media_dict)
                    if type(tweets) == list:
                        for obj_ in tweets:
                            tweets_dict, retweets_dict, replies_dict, quotes_dict = extractTweetsFromDict(obj_, tweets_dict, True, users_dict, places_dict, retweets_dict, replies_dict, quotes_dict, media_dict)
                    else:
                        tweets_dict, retweets_dict, replies_dict, quotes_dict = extractTweetsFromDict(tweets, tweets_dict, True, users_dict, places_dict, retweets_dict, replies_dict, quotes_dict, media_dict)
                        
            #print(f'lines : {lines} with tweets : {len(tweets)} and tweets_dict: {len(tweets_dict)} and retweets_dict: {len(retweets_dict)}')
            

            
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
            
            
            OUTPUT_SUB_FOLDER = workFile.replace(f'{data_path}/RAW_DATA_','').replace(f'{data_path}/RAW_SEARCH_','')
            
            if len(OUTPUT_SUB_FOLDER) > 10:
                OUTPUT_SUB_FOLDER = OUTPUT_SUB_FOLDER[:(10-len(OUTPUT_SUB_FOLDER))]
            
            folderToCompress = join(data_path, OUTPUT_SUB_FOLDER)            
            outputFile = join(data_path, OUTPUT_SUB_FOLDER) 
            outputFile = outputFile.replace('streaming', 'processed')
            print(f'workFile: {workFile} --> outputFile : {outputFile} --> folderToCompress : {folderToCompress}')
            
            if os.path.exists(folderToCompress):
                if isfile(folderToCompress):
                    shutil.move(folderToCompress, folderToCompress + '__')
                    os.makedirs(folderToCompress)
                    shutil.move(folderToCompress + '__', join(folderToCompress, OUTPUT_SUB_FOLDER))
            else:
                os.makedirs(folderToCompress)
            
            
            with open(outputFile.replace('streaming', 'processed'), 'a+', encoding='utf-8') as fout:
                for k in combined_dict.keys():
                    fout.write(f"{json.dumps(combined_dict[k], ensure_ascii=False)}\n")
            
            try:
                if limit not in workFile:
                    if os.path.exists(f"{workFile}") :
                        shutil.move(f"{workFile}", f"{workFile.replace(data_path, folderToCompress)}")
            except Exception as exp1:
                handleException(exp1,str(workFile),func_=f'__main__\n{__name__}')
            
    
    folders_ = [f for f in listdir(data_path) if not isfile(join(data_path, f)) and not limit_day in f and  not f.endswith('.gz')]
    for folder in folders_:
        print(f'folder: {folder}')
        if limit_day not in folder:
            compressed = False
            outputFileCompress = f"{join(data_path, folder)}.tar.gz"
            x = 1
            while (os.path.exists(outputFileCompress)):
                outputFileCompress = f"{join(data_path, folder)}_{x}.tar.gz"
                x += 1
            with tarfile.open(outputFileCompress, "w:gz") as tar:
                tar.add(join(data_path, folder), arcname=os.path.basename(join(data_path, folder)))
                compressed = True
            if compressed:
                if os.path.exists(join(data_path, folder)):
                    if isfile(join(data_path, folder)):
                        os.remove(join(data_path, folder))
                    else:
                        shutil.rmtree(join(data_path, folder))
