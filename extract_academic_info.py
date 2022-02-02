import pandas as pd
import numpy as np
import time
import os
from urllib.request import urlopen
import json 
from nltk.tokenize import TweetTokenizer
tweet_tokenizer = TweetTokenizer()
from os import listdir
from os.path import isfile, join

data_path = "../data_sample/"
OUTPUT_FOLDER = '../data_sample/data/'


if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)


tweets_files = [join(data_path, f) for f in listdir(data_path) if isfile(join(data_path, f)) and f.startswith('tweets')]
users_files = [join(data_path, f) for f in listdir(data_path) if isfile(join(data_path, f)) and f.startswith('users')]
includes_files = [join(data_path, f) for f in listdir(data_path) if isfile(join(data_path, f)) and f.startswith('includes')]


def get_json_content(file_name):
    temp_dict = dict()
    print(file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        for item in f:
            object_ = json.loads(item)
            temp_dict[object_['id']] = object_#['text']
    
    return temp_dict

def extract_users_contents(users_file=''):
    users_dict = get_json_content(users_file)
    for k in users_dict.keys():
        if k not in combined_users_dict2.keys():
            combined_users_dict2[k] = users_dict[k]
            
            
def extract_tweets_contents2(tweets_file=''):
    
    tweets_dict = get_tweets_from_json(tweets_file)
    
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
                                if tweets_dict[k]['author_id'] not in combined_tweets_dict2[referenced_tweet['id']]['retweeters']:
                                    combined_tweets_dict2[referenced_tweet['id']]['retweeters'].append(tweets_dict[k]['author_id'])

                elif referenced_tweet['type'] == 'replied_to':
                    combined_tweets_dict2[k] = {'id':includes_dict[k]['id'], 'text':includes_dict[k]['text']}
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

def write_tweets_and_users(type_ = 'csv'):
    if type_ == 'csv':
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
            df.to_csv(OUTPUT_FOLDER+'combined.csv', encoding='utf-8', index=False)
        except Exception as exp:
            print(exp)
    else:
        with open(OUTPUT_FOLDER+'combined.json','w',encoding='utf-8') as fout:
            for k in combined_tweets_dict2.keys():
                fout.write('%s\n'%json.dumps(combined_tweets_dict2[k],ensure_ascii=False))
                
                



tweets_dict = dict()
includes_dict = dict()
users_dict = dict()

combined_users_dict2 = dict()

#def extract_users_main():
for file in users_files:
    extract_users_contents(users_file=file)

users_df = pd.DataFrame.from_dict(combined_users_dict2, orient='index')

users_df.to_pickle(OUTPUT_FOLDER+'users_df.pickle')


def get_tweets_from_json(file_name):
    temp_dict = dict()
    print(file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        for item in f:
            object_ = json.loads(item)
            if object_['id'] not in temp_dict.keys():
                temp_dict[object_['id']] = {'id':object_['id'],
                                            'created_at':time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ')), 
                                            'author_id':object_['author_id'], 
                                            'mentions':[x for x in tweet_tokenizer.tokenize(object_['text']) if x.startswith('@')], 
                                            'text': object_['text'], 
                                            'hashtags':[x.replace('#','').strip().lower() for x in tweet_tokenizer.tokenize(object_['text']) if x.startswith('#') and len(x)>1]}
                if 'in_reply_to_user_id' in object_.keys():
                    temp_dict[object_['id']]['reply_to'] = object_['in_reply_to_user_id']
    return temp_dict

def extract_includes_contents(includes_file=''):
    includes_dict = get_tweets_from_json(includes_file)
    for k in includes_dict.keys():
        if k not in combined_tweets_dict2.keys():
            combined_tweets_dict2[k] = includes_dict[k]

def extract_tweets_contents(tweets_file=''):
    tweets_dict = get_tweets_from_json(tweets_file)
    for k in tweets_dict.keys():
        if k not in combined_tweets_dict2.keys():
            combined_tweets_dict2[k] = tweets_dict[k]

            
            
            
combined_tweets_dict2 = dict()


#def extract_includes_main():
for file in tweets_files:
    extract_includes_contents(includes_file=file.replace('tweets','includes'))
    
    
for k in combined_tweets_dict2.keys():
    if combined_tweets_dict2[k]['author_id'] in combined_users_dict2.keys():
        combined_tweets_dict2[k]['author'] = combined_users_dict2[combined_tweets_dict2[k]['author_id']]['username']  
        
        
included_tweets_df = pd.DataFrame.from_dict(combined_tweets_dict2, orient='index')

included_tweets_df.to_pickle(OUTPUT_FOLDER+'included_tweets_df.pickle')



#def extract_tweets_main(tweets_files)
for file in tweets_files:
    extract_tweets_contents(tweets_file=file)
    
for k in combined_tweets_dict2.keys():
    if combined_tweets_dict2[k]['author_id'] in combined_users_dict2.keys():
        combined_tweets_dict2[k]['author'] = combined_users_dict2[combined_tweets_dict2[k]['author_id']]['username']
    
    
tweets_df = pd.DataFrame.from_dict(combined_tweets_dict2, orient='index')

tweets_df.to_pickle(OUTPUT_FOLDER+'tweets_df.pickle')



#def filter_dataFrame_on_hashtags(tweets_df):
hashtags_ = list(tweets_df.hashtags)
hashtags_dict = dict()

for item in hashtags_:
    for hashtag in item:
        if len(hashtag) > 1:
            if hashtag.strip().lower() in hashtags_dict.keys():
                hashtags_dict[hashtag.strip().lower()] += 1
            else:
                hashtags_dict[hashtag.strip().lower()] = 1
            
hashtags_dict = {k: v for k, v in sorted(hashtags_dict.items(), key=lambda item: item[1], reverse=True)}

    
    
def update_date_format(date_):
    return time.strftime('%Y', time.strptime(date_,'%Y-%m-%dT%H:%M:%SZ'))

tweets_with_hashtags = tweets_df[tweets_df.hashtags.astype(bool)]
tweets_with_hashtags['created_at']= tweets_with_hashtags.created_at.apply(update_date_format)


years_list = ['2018','2019','2020','2021']
tweets_dict_cleaned = dict()
for year in years_list:
    tweets_dict_cleaned[year] = tweets_with_hashtags[tweets_with_hashtags['created_at'] ==year].drop(axis=1,columns=['id','author_id','mentions','author','reply_to']).to_dict('index')
    
hashtags_dict_by_year = dict()

for year in years_list:
    if year not in hashtags_dict_by_year.keys():
        hashtags_dict_by_year[year] = dict()
    for k in tweets_dict_cleaned[year].keys():
        for hashtag in tweets_dict_cleaned[year][k]['hashtags']:
            if hashtag not in hashtags_dict_by_year[year]:
                hashtags_dict_by_year[year][hashtag] = [k]
            else:
                hashtags_dict_by_year[year][hashtag].append(k)
                
                
for year in hashtags_dict_by_year.keys():
    for hashtag in hashtags_dict_by_year[year].keys():
        hashtags_dict_by_year[year][hashtag] = list(set(hashtags_dict_by_year[year][hashtag]))
        
for year in hashtags_dict_by_year.keys():
    if not os.path.exists(OUTPUT_FOLDER + 'lists'):
        os.makedirs(OUTPUT_FOLDER + 'lists')
        
    with open(OUTPUT_FOLDER + 'lists/hashtags' + year + '.json', 'w', encoding='utf-8') as fout:
        fout.write('{}'.format(json.dumps(hashtags_dict_by_year[year],ensure_ascii=False)))
    
    hashtags_length_by_year = dict()
    for k in hashtags_dict_by_year[year].keys():
        hashtags_length_by_year[k] = len(hashtags_dict_by_year[year][k])
    
    hashtags_length_by_year = {k: v for k, v in sorted(hashtags_length_by_year.items(), key=lambda item: item[1],reverse=True) if v >= 10}
    
    if not os.path.exists(OUTPUT_FOLDER + 'lengths'):
        os.makedirs(OUTPUT_FOLDER + 'lengths')        
    with open(OUTPUT_FOLDER + 'lengths/hashtags' + year + '.json', 'w', encoding='utf-8') as fout:
        fout.write('{}'.format(json.dumps(hashtags_length_by_year,ensure_ascii=False)))
        
        
years = ['2018_2019','2018_2020','2018_2021','2019_2020','2019_2021','2020_2021','2018_2019_2020','2018_2019_2021','2018_2020_2021','2019_2020_2021','2018_2019_2020_2021']
for year_com in years:
    hashtags_dict_temp = dict()
    for year in year_com.split('_'):
        for k in hashtags_dict_by_year[year].keys():
            if k not in hashtags_dict_temp.keys():
                hashtags_dict_temp[k] = hashtags_dict_by_year[year][k]
            else:
                hashtags_dict_temp[k].extend(hashtags_dict_by_year[year][k])     
    
    for k in hashtags_dict_temp.keys():
        hashtags_dict_temp[k] = list(set(hashtags_dict_temp[k]))
    
    if not os.path.exists(OUTPUT_FOLDER + 'lists'):
        os.makedirs(OUTPUT_FOLDER + 'lists')
    with open(OUTPUT_FOLDER + 'lists/hashtags' + year_com + '.json', 'w', encoding='utf-8') as fout:
        fout.write('{}'.format(json.dumps(hashtags_dict_temp,ensure_ascii=False)))
    
    for k in hashtags_dict_temp.keys():
        hashtags_dict_temp[k] = len(hashtags_dict_temp[k])    
    
    hashtags_dict_temp = {k: v for k, v in sorted(hashtags_dict_temp.items(), key=lambda item: item[1],reverse=True) if v >= 10}
    
    if not os.path.exists(OUTPUT_FOLDER + 'lengths'):
        os.makedirs(OUTPUT_FOLDER + 'lengths')    
    with open(OUTPUT_FOLDER + 'lengths/hashtags' + year_com + '.json', 'w', encoding='utf-8') as fout:
        fout.write('{}'.format(json.dumps(hashtags_dict_temp,ensure_ascii=False)))
        
        
for year in hashtags_dict_by_year.keys():
    hashtags_dict_ = dict()
    for hashtag in hashtags_dict_by_year[year].keys():
        if hashtag not in hashtags_dict_.keys():
            hashtags_dict_[hashtag] = dict()
        for tid in hashtags_dict_by_year[year][hashtag]:
            hashtags_dict_[hashtag][tid] = tweets_dict_cleaned[year][tid]['text']
            
    for hashtag in hashtags_dict_.keys():
        with open(OUTPUT_FOLDER + 'tweets/' + year + '/' + hashtag+'.json', 'w', encoding='utf-8') as fout:
            fout.write('{}'.format(json.dumps(hashtags_dict_[hashtag],ensure_ascii=False)))