#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Created on Sat Jan 11 00:00:00 2020

"""
import os
import re
import sys
import json
import time
import nltk
import email
import smtplib
import requests
tokenizer = nltk.TweetTokenizer()
import pandas as pd
import fasttext
from nltk.tokenize import TweetTokenizer
tweet_tokenizer = TweetTokenizer()
from datetime import datetime
from os.path import exists, join
from os import path, listdir, remove
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from logging.handlers import TimedRotatingFileHandler as _TimedRotatingFileHandler
class TimedRotatingFileHandler(_TimedRotatingFileHandler):
    """A class to manage the backup compression.
    Args:
        _TimedRotatingFileHandler ([type]): [description]
    """
    def __init__(self, filename="", when="midnight", interval=1, backupCount=0):
        super(TimedRotatingFileHandler, self).__init__(
            filename=filename,
            when=when,
            interval=int(interval),
            backupCount=int(backupCount))
    
    def doRollover(self):
        import subprocess
        super(TimedRotatingFileHandler, self).doRollover()

def create_logger(name, level=logging.INFO, file=None):
    '''
    A function to log the events. Mainly used to manage writing to log file and to manage the files compression through TimedRotatingFileHandler class.
    
    Parameters
    ----------
    name : String
        Logger name.
    level : optional.
        level of logging (info, warning). The default is logging.INFO.
    file : String, optional
        File name, the name of the logging file. The default is None where no compression will be set in file is None.

    Returns
    -------
    logger after creation.
    '''
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logging_formatter = logging.Formatter(
        '[%(asctime)s - %(name)s - %(levelname)s] '
        '%(message)s',
        '%Y-%m-%d %H:%M:%S')
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging_formatter)
    logger.addHandler(ch)
    
    if file:
        #file_handler = logging.FileHandler("{}.log".format(file))
        file_handler = TimedRotatingFileHandler(filename=f"../.logs/{file}.log", when='midnight', interval=1, backupCount=0)#when midnight, s (seconds), M (minutes)... etc
        file_handler.setFormatter(logging_formatter)
        logger.addHandler(file_handler)
    return logger
logger = create_logger('util', file='util')


lang_model_file = "../../assets/lid.176.bin"
lang_model = fasttext.load_model(lang_model_file)
language_dict = {'Non_Text':'Non_Text', 'af':'afrikaans','sq':'albanian','am':'amharic','ar':'arabic','arz':'arabic','an':'aragonese','hy':'armenian','as':'assamese','av':'avaric','az':'azerbaijani','ba':'bashkir','eu':'basque','be':'belarusian','bn':'bengali','bh':'bihari','bs':'bosnian','br':'breton','bg':'bulgarian','my':'burmese','ca':'catalan','ce':'chechen','zh':'chinese','cv':'chuvash','kw':'cornish','co':'corsican','hr':'croatian','cs':'czech','da':'danish','dv':'divehi','nl':'dutch','en':'english','eo':'esperanto','et':'estonian','fi':'finnish','fr':'french','gl':'galician','ka':'georgian','de':'german','el':'greek','gn':'guarani','gu':'gujarati','ht':'haitian','he':'hebrew','hi':'hindi','hu':'hungarian','ia':'interlingua','id':'indonesian','ie':'interlingue','ga':'irish','io':'ido','is':'icelandic','it':'italian','ja':'japanese','jv':'javanese','kn':'kannada','kk':'kazakh','km':'khmer','ky':'kirghiz','kv':'komi','ko':'korean','ku':'kurdish','la':'latin','lb':'luxembourgish','li':'limburgan','lo':'lao','lt':'lithuanian','lv':'latvian','gv':'manx','mk':'macedonian','mg':'malagasy','ms':'malay','ml':'malayalam','mt':'maltese','mr':'marathi','mn':'mongolian','ne':'nepali','nn':'norwegian','no':'norwegian','oc':'occitan','or':'oriya','os':'ossetian','pa':'punjabi','fa':'persian','pl':'polish','ps':'pashto','pt':'portuguese','qu':'quechua','rm':'romansh','ro':'romanian','ru':'russian','sa':'sanskrit','sc':'sardinian','sd':'sindhi','sr':'serbian','gd':'gaelic','si':'sinhala','sk':'slovak','sl':'slovenian','so':'somali','es':'spanish','su':'sundanese','sw':'swahili','sv':'swedish','ta':'tamil','te':'telugu','tg':'tajik','th':'thai','bo':'tibetan','tk':'turkmen','tl':'tagalog','tr':'turkish','tt':'tatar','ug':'uyghur','uk':'ukrainian','ur':'urdu','uz':'uzbek','vi':'vietnamese','wa':'walloon','cy':'welsh','fy':'frisian','yi':'yiddish','yo':'yoruba', 'lang':'english', 'en-gb' : 'english uk', 'fil' : 'filipino', 'msa' : 'malay', 'zh-cn' : 'simplified chinese', 'zh-tw' : 'traditional chinese'}
language_dict_inv = {'Non_Text':'Non_Text', "afrikaans":"af","albanian":"sq","amharic":"am","arabic":"ar","aragonese":"an","armenian":"hy","assamese":"as","avaric":"av","azerbaijani":"az","bashkir":"ba","basque":"eu","belarusian":"be","bengali":"bn","bihari":"bh","bosnian":"bs","breton":"br","bulgarian":"bg","burmese":"my","catalan":"ca","chechen":"ce","chinese":"zh","chuvash":"cv","cornish":"kw","corsican":"co","croatian":"hr","czech":"cs","danish":"da","divehi":"dv","dutch":"nl","english":"en","esperanto":"eo","estonian":"et","finnish":"fi","french":"fr","galician":"gl","georgian":"ka","german":"de","greek":"el","guarani":"gn","gujarati":"gu","haitian":"ht","hebrew":"he","hindi":"hi","hungarian":"hu","interlingua":"ia","indonesian":"id","interlingue":"ie","irish":"ga","ido":"io","icelandic":"is","italian":"it","japanese":"ja","javanese":"jv","kannada":"kn","kazakh":"kk","khmer":"km","kirghiz":"ky","komi":"kv","korean":"ko","kurdish":"ku","latin":"la","luxembourgish":"lb","limburgan":"li","lao":"lo","lithuanian":"lt","latvian":"lv","manx":"gv","macedonian":"mk","malagasy":"mg","malay":"ms","malayalam":"ml","maltese":"mt","marathi":"mr","mongolian":"mn","nepali":"ne","norwegian":"nn","occitan":"oc","oriya":"or","ossetian":"os","punjabi":"pa","persian":"fa","polish":"pl","pashto":"ps","portuguese":"pt","quechua":"qu","romansh":"rm","romanian":"ro","russian":"ru","sanskrit":"sa","sardinian":"sc","sindhi":"sd","serbian":"sr","gaelic":"gd","sinhala":"si","slovak":"sk","slovenian":"sl","somali":"so","spanish":"es","sundanese":"su","swahili":"sw","swedish":"sv","tamil":"ta","telugu":"te","tajik":"tg","thai":"th","tibetan":"bo","turkmen":"tk","tagalog":"tl","turkish":"tr","tatar":"tt","uyghur":"ug","ukrainian":"uk","urdu":"ur","uzbek":"uz","vietnamese":"vi","walloon":"wa","welsh":"cy","frisian":"fy","yiddish":"yi","yoruba":"yo","english uk":"en-gb","filipino":"fil","msa":"malay","zh-cn":"simplified chinese","zh-tw":"traditional chinese"}

tweets_list = [] 

def get_language(tweet_text):
    """function to extract the language of the passed string.
    It is based on fasttext language identification and uses the libraries (fasttext, re) in python.
    Proceudre:
        1- remove hashtags, mentions and urls
        2- remove non-alpha characters
        3- predict the language
        4- in case of errors, return english with 0 confidence.
    Args:
        tweet_text (str): The string that you need to find its language.

    Returns:
        List: List of lists that contains the identified language with its considence. examples: [['english',0.9]] or [['english',0.6],['spanish',0.3]]
    """
    detect_lang = 'Non_Text'
    try:
        tweet_text = re.sub('[@#][^ ]+',' ',re.sub('http[s]:[^ ]+',' ',tweet_text))
        tweet_text = re.sub('[\n]+',' ',tweet_text)
        tweet_text = re.sub('[ًٌٍَُِّْ]+','',tweet_text)
        tmp = ' '.join([x for x in tweet_text.split() if str.isalpha(x) and not x.startswith('#') and not x.startswith('@')])
        if len(tmp.split()) > 3:
            tweet_text = tmp    #this to avoid removing Ch/Ja/Ta strings
        
        if len(re.sub('[ ]+',' ',tweet_text).strip().split(' ')) > 0:
            lang = lang_model.predict(tweet_text,1,0.2)
            l = lang[0]
            for i in range(len(l)):
                a = str(l[i]).replace('label','').replace('_','')
                if a in language_dict.keys():
                    detect_lang = language_dict[a]
                    break
                else:
                    return 'Non_Text'
    except Exception as exp:
        detect_lang = 'Non_Text' #if exception occured, set langauge to empty field
        handleException(exp,func_=__name__)
    return detect_lang

def get_sentiments(tweets):
    """A function to access sentiment analysis service.

    'id': tweet['id'], "full_text": item["full_text"], "language"

    Args:
        tweets (dict): A dictionary of the tweets object. It should have the following keys:
        1) 'id': tweet id, 
        2) 'full_text': the full_text of the tweet,
        3) 'language': the detected language of the tweet.

    Returns:
        dict: A dictionary that hold the sentiment information as retrived from its service. The keys are the tweets ids and values are dicts that contain:
        'sentiment' : the sentiment information as being analysed from the text, (positive, nuetral or negative)
        'sentiment_distribution' : a list that has the distribution of the three sentiments (the highest would be at the index of the selected sentiment)
    """
    headers = {'content-type': 'application/json; charset=utf-8'}
    url_sent = "http://127.0.0.1:7777/api/predict"
    data = json.dumps(tweets, ensure_ascii=False)
    
    rs = -1
    try:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print('Send to SA; Current Time =', current_time)
        response = requests.post(url=url_sent, headers = headers , data=data.encode('utf-8'))
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print('SA finished; Current Time =', current_time)
        rs = response.status_code
    except Exception as exp: 
        raise exp
        
    if rs != 200:
        print('Sentiment analyzer not working!.. Error code: ' + str(rs))
        #logger.warning(f'[get_sentiments]: Sentiment analyzer not working!.. Error code: {str(rs)}')
        time.sleep(3)
        return None
    return json.loads(response.content)

def get_location(tweets):
    """A function to access location service.

    Args:
        tweets (dict): A dictionary of the tweets object. It should have the following keys:
        1) 'id': tweet id, 
        2) 'user': the user object as exists in the tweet object,
        3) 'geo': the geo field from the tweet,
        4) 'coordinates': the coordinates field from the tweet, 
        5) 'place': the place field from the tweet, 
        6) 'language': the detected language of the tweet.

    Returns:
        dict: A dictionary that hold the location information as retrived from location service. The keys are the tweets ids and values are dicts that contain
        'user' : the location information from user object
        'tweet' : the location information from the tweet object (location_gps)
        'language' (optional): the location as extracted from the tweets' language
    """
    url1 = "http://127.0.0.1:10000/api/get_locations"
    '''
    with open('sample.json' , 'a' , encoding='utf-8') as fout:
        fout.write('%s\n'%json.dumps(tweets,ensure_ascii=False))
    '''
    data = json.dumps(tweets,ensure_ascii=False)
    headers = {'content-type': 'application/json; charset=utf-8'}
    # sending get request and saving the response as response object
    rs = -1
    trials = 1
    while (rs != 200 and trials <= 3):
        try:
            response = requests.post(url=url1, data=data.encode('utf-8'), headers=headers)
            rs = response.status_code
        except Exception as exp:
            print(exp)
            rs = -1
        finally:
            trials += 1
    if rs != 200:
        #logger.warning(f'[get_location]: Location service not found. Error code: ' + str(rs))
        return None
    return json.loads(response.content)

def get_urls_from_object(tweet_obj):
    """Extract urls from a tweet object

    Args:
        tweet_obj (dict): A dictionary that is the tweet object, extended_entities or extended_tweet

    Returns:
        list: list of urls that are extracted from the tweet.
    """
    url_list = []
    if 'entities' in tweet_obj.keys():
        if 'urls' in tweet_obj['entities']:
            for url_ in tweet_obj['entities']['urls']:
                if 'expanded_url' in url_.keys():
                    url_list.extend([url_['expanded_url']])
                elif 'display_url' in url_.keys():
                    url_list.extend([url_['display_url']])
                elif 'url' in url_.keys():
                    url_list.extend([url_['url']])
            url_list = list(set(url_list))
    return url_list
    
def update_log(file_name, updates):
    print(updates,'\n')
    with open(file_name, 'a+',encoding='utf-8') as logging:
        logging.write("%s%s"%(updates,'\n'))

def writeRunningStatus(status_file,status):
    with open(status_file,'w') as f_out:
        f_out.write(status)
    return True

def readRunningStatus(status_file):
    try:
        with open(status_file,'r') as f_in:
            kRun = f_in.readline()
        return kRun.strip() == '1'
    except Exception as exp:
        return False

def dumpDictToFile(output_file, data_dict, mode = 'w',encoding='utf-8'):
    with open(output_file,mode,encoding='utf-8') as fo:
        json.dump(data_dict,fp=fo, ensure_ascii=False)

def load_api_keys(keys="keys", index=0):
    df = pd.DataFrame()
    api_keys = {"apikeys":dict()}
    try:
        df = pd.read_csv(keys,sep=',')
        if not df.empty:
            for item in df.iterrows():
                if item[0] % 2 == index:
                    api_keys["apikeys"][item[0]] = dict(item[1])
    except Exception as exp:
        print('error while loading API keys... ' + str(exp))
        pass
    return api_keys

def write_location_to_solr(tweets_list, solr, max_row):
    try:        
        status = ''
        i = 0
        print('write data to solr, ',len(tweets_list))
        while ('"status">0<' not in status and i < 3):
            status = solr.add(tweets_list, softCommit=False , fieldUpdates={'user_location':'set', 'location_gps':'set', 'emotion':'set'})
            i+=1
        if '"status">0<' not in status:
            logger.warning(f'[write_location_to_solr]: Error occured, server response: {status}')
            print(f'[write_location_to_solr]: Error occured, server response: {status}')
            return False
        else:
            print('Location update Done for ' + str(len(tweets_list)) + ' out of ' + str(max_row))
            logger.info(f'[write_location_to_solr]: Done for {str(len(tweets_list))} out of {str(max_row)}')
            print(f'[write_location_to_solr]: Done for {str(len(tweets_list))} out of {str(max_row)}')
            sys.stdout.flush()
            return True
    except Exception as exp:
        print('Exception ', str(exp), ' occured, try later')
        logger.warning(f'[write_location_to_solr]: Exception occured: {str(exp)}')
        print(f'[write_location_to_solr]: Exception occured: {str(exp)}')
        return False
        
def write_sentiment_to_solr(tweets_list, solr, max_row, sleep_time):
    try:        
        status = ''
        i = 0
        print('write data to solr, ',len(tweets_list))
        while ('"status">0<' not in status and i < 3):
            status = solr.add(docs=tweets_list, softCommit=False, fieldUpdates={'sentiment':'set', 'language_s':'set'})
            #status = '"status">0<'
            i+=1
        if '"status">0<' not in status: #Only add the missed information when solr not accessed proberly.
            logger.warning(f'[write_sentiment_to_solr]: Error occured try again later. Sleeping for {sleep_time}.')
            return False
        else:
            now = datetime.now()
            logger.info(f'[write_sentiment_to_solr]: Done {str(len(tweets_list))} out of {str(max_row)}')
            print('Done ', str(len(tweets_list)), ' out of ', str(max_row), 'Current Time =', now)
            return True, 3
    except Exception as exp:
        print('Exception at update_sentiment line 122\n',exp)
        logger.warning(f'[write_sentiment_to_solr]: Exception at update_sentiment: {exp}. Sleeping for {sleep_time}.')
        return False
    

def get_search_dict(terms_file, since_id=1, geocode = None):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    '''
    terms_list = []
    with open (terms_file, 'r') as f_in:
        for line in f_in.readlines():
            terms_list.append(line)
    import math
    x = math.ceil(len(terms_list) / 15)
    data_dict = {}
    for i in range(0,x):
        data_dict['search'+str(i)] = dict()
        data_dict['search'+str(i)]["geocode"] = geocode
        data_dict['search'+str(i)]["since_id"] = since_id
        data_dict['search'+str(i)]["terms"] = []
        for j in range(0,15):
            if j+15*i < len(terms_list):
                data_dict['search'+str(i)]["terms"].append(terms_list[j+15*i])
    return data_dict

def get_screen_name_dict(screen_name_file, since_id=1, geocode = None):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    '''
    screen_names_dict = {}
    with open (screen_name_file, 'r') as f_in:
        screen_names_dict = json.load(f_in)
    return screen_names_dict
    
def get_ids_dict(ids_file, since_id=1, geocode = None):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    '''
    ids_dict = {}
    with open (ids_file, 'r') as f_in:
        ids_dict = json.load(f_in)
    return ids_dict

def update_screen_name_file(screen_names_dict, screen_name_file):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    '''
    for k in screen_names_dict.keys():
        if "since_id" not in screen_names_dict[k].keys():
            screen_names_dict[k]["since_id"] = 1
    with open (screen_name_file, 'w') as f_out:
        json.dump(screen_names_dict, f_out, ensure_ascii = False)

def get_terms_list(terms_file):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    '''
    terms_list = []
    with open (terms_file, 'r', encoding='utf-8') as f_in:
        for line in f_in.readlines():
            terms_list.append(line.strip())
    return terms_list

def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0
    
def get_services(config_file):
    services = []
    if is_non_zero_file(config_file):
        df = pd.read_csv(config_file, sep='\n')
        services = list(df['core_name'].values.tolist())
    return services

def create_headers(BEARER_TOKENS):
    if type(BEARER_TOKENS) == str:
        headers = {"Authorization": "Bearer {}".format(BEARER_TOKENS)}
    else:
        headers = []
        for BEARER_TOKENS in BEARER_TOKENS:
            headers.append({"Authorization": "Bearer {}".format(BEARER_TOKENS)})
    return headers

def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response


def writeDataToFile(objects, file_name, folder):
    """A function to write data into a file

    Args:
        objects (Dict or List): the dictionary of the tweets or users with their extracted information.
        file_name (str): the file name in which the data will be writen to.
        folder (str): the folder in which the file will be written to.
    """
    if objects:
        if not path.exists(f'{folder}'):
            os.mkdir(f'{folder}')
        with open(f'{join(folder, file_name)}','a+',encoding='utf-8') as fout:
            if type(objects) == dict:
                fout.write('%s\n'%json.dumps(objects, ensure_ascii=False))
            elif type (objects) == list:
                for object_ in objects:
                    fout.write('%s\n'%json.dumps(object_, ensure_ascii=False))

def update_log_excption(command, exc):
    if (command == 'terms'):
        update_log(streamer_log, exc)
    if (command == 'search'):
        update_log(crawler_log, exc)

def getJSONContent(folder, file_name):
    temp_dict = dict()
    print(file_name)
    with open(folder +'/'+ file_name, 'r', encoding="utf-8") as fin:
        for line in fin.readlines():
            object_ = json.loads(line)
            print('type of object_: {}'.format(type(object_)))
            try:
                temp_dict[object_['id']] = object_
            except Exception as exp:
                pass
    return temp_dict

def handleException(exp, object_='Unknown', func_= 'Unknown'):
    print(f'Error {exp}\n')
    exception_type, exception_object, exception_traceback = sys.exc_info()
    line_number = exception_traceback.tb_lineno
    
    print(f"------------------------------------------------\nException type: {exception_type}\n \
        Line number: {line_number}.\nexception_object: {exception_object}\n \
            Exception message : {exp}\nObject: {object_}\nFunction: {func_}.\
                \n================================================")
    
def sendNotificationEmail(title, subject, email_body, emailaddress='y.alhariri@outlook.com', KEY=''):
	# email     
	msg = MIMEMultipart('alternative') #Create Multipart msg (allows html)
	msg['To'] = email.utils.formataddr(('Recipient', emailaddress )) #'benyoucef021@gmail.com'))
	msg['From'] = email.utils.formataddr((title, 'donotreply.diil@gmail.com'))
	msg['Subject'] = subject
	part_html = MIMEText(email_body, 'html')
	msg.attach(part_html)

	server = smtplib.SMTP('smtp.gmail.com',587)
	server.ehlo()
	server.starttls()
	server.login('donotreply.damasus22@gmail.com',KEY)
	server.set_debuglevel(False) # show communication with the server
	try:
		server.sendmail('donotreply.damasus22@gmail.com', emailaddress, msg.as_string())
	finally:
		server.quit()

def exitHandler(project, tool,key):
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    time = now.strftime("%Y-%m-%d %H:%M:%S")
		
    message = f' WARNING : {project} {tool} has stopped at time {time}\n please check the server.'
    email_body =  f'<html><body><div style="text-align: center; font-family: serif; font-size: 15px;"><br/><br/>{project} {tool} Notification<br/><br/>{message}<br/><br/><br/><br/> <br/><br/></div></body></html>' 
    
    title= f'{project} {tool} has stopped'
    subject= f'{project} {tool} has stopped - Please take action!!!!'

    sendNotificationEmail(title, subject, email_body, emailaddress='y.alhariri@outlook.com', KEY=key)
    
    print (f'Code has stopped on : {time}... Notification email sent')

def confirmALife(project,tool, key):
    now = datetime.now()
    time = now.strftime("%Y-%m-%d %H:%M:%S")
		
    message = f' INFORMATION : {project} {tool} started at time {time}\n no need to check the server!'
    email_body =  f'<html><body><div style="text-align: center; font-family: serif; font-size: 15px;"><br/><br/>{project} {tool} Notification<br/><br/>{message}<br/><br/><br/><br/> <br/><br/></div></body></html>' 
    
    title= f'{project} {tool} started'
    subject= f'{project} {tool} started - No action required.'
    sendNotificationEmail(title, subject, email_body, emailaddress='y.alhariri@outlook.com', KEY=key)

def getPlatform(source = '<PLT_1>'):
    """A function to extract the platform from a source string.
    Args:
        source (str, optional): source string that is usually contains the platform that is used to post the tweet. Defaults to '<PLT_1>'.
    Returns:
        str: the platform if found, otherwise the stamp PLT_1. This stamp is used for any further updates.
    """
    platform = ''
    try:
        platform = re.sub('[<>]', '\t', source).split('\t')[2]
        platform = platform.replace('Twitter for','').replace('Twitter','')
    except:
        platform = ''
    return platform.strip()


def getEmotion(text = ''):
    """
    -- Not implemented yet --
    A function to extract the emotion from a text string.
    Args:
        text (str, optional): text string of the tweet. Defaults to ''.
    Returns:
        str: the emotion.
    """
    return ''

'''
def extract_users_contents(file_name='', users_dict=dict()):
    with open(file_name, 'r', encoding='utf-8') as f:
        for item in f:
            try:
                object_ = json.load(item)
                if object_['id'] not in users_dict.keys():
                    users_dict[object_['id']] = object_
            except Exception as exp:
                try:
                    object_ = json.loads(item.replace('%s',''))
                    if object_['id'] not in users_dict.keys():
                        users_dict[object_['id']] = object_
                except Exception as exp:
                    try:
                        object_ = json.loads(item)
                        if object_['id'] not in users_dict.keys():
                            users_dict[object_['id']] = object_
                    except Exception as exp:
                        handleException(exp,object_,__name__)
    return users_dict
'''

def getMediaFromObject(media_dict, media_keys):
    """Extract urls from a tweet object
    Args:
        media_keys (dict): A dictionary that holds the media_keys
    Returns:
        list: list of the media urls that are extracted from the tweet.
    """
    media_list = []
    try:
        for item in media_keys:
            if item in media_dict.keys():
                item_ = media_dict[item]
                if "url" in item_.keys():
                    media_list.append(item_['url'])
    except Exception as exp:
        handleException(exp,media_keys,__name__)
    return media_list

def extractMediaContents(file_name='', media_dict=dict()):
    """Extract media objects from a file
    Args:
        file (str): The path of the file.
        media_dict (dict): A dictionary that is the media objects.
    Returns:
        media_dict (dict): The updated dictionary that is the media objects.
    """
    with open(file_name, 'r', encoding='utf-8') as fin:
        for item in fin:
            try:
                object_ = json.loads(item)
                if object_['media_key'] not in media_dict.keys():
                    if 'url' in object_.keys():
                        media_dict[object_['media_key']] = object_
            except Exception as exp:
                handleException(exp,object_,__name__)
    return media_dict

def extractMediaContentsFromDict(items=None, media_dict=dict()):
    """Extract media objects from a file
    Args:
        file (str): The path of the file.
        media_dict (dict): A dictionary that is the media objects.
    Returns:
        media_dict (dict): The updated dictionary that is the media objects.
    """
    if items:
        for item in items:
            try:
                if item['media_key'] not in media_dict.keys():
                    if 'url' in item.keys():
                        media_dict[item['media_key']] = item
            except Exception as exp:
                handleException(exp,item,__name__)
    return media_dict


'''
def extract_places_contents(file_name='', places_dict=dict()):
    with open(file_name, 'r', encoding='utf-8') as f:
        for item in f:
            try:
                object_ = json.loads(item.replace('%s',''))
                if object_['id'] not in places_dict.keys():
                    places_dict[object_['id']] = object_
            except Exception as exp:
                try:
                    object_ = json.loads(item)
                    if object_['id'] not in places_dict.keys():
                        places_dict[object_['id']] = object_
                except Exception as exp:
                    try:
                        object_ = json.load(item)
                        if object_['id'] not in places_dict.keys():
                            places_dict[object_['id']] = object_
                    except Exception as exp:
                        handleException(exp,object_,__name__)
    return places_dict
'''

def extractResponseContents(file_name='', objects_dict=dict()):
    with open(file_name, 'r', encoding='utf-8') as f:
        for item in f:
            try:
                object_ = json.loads(item.replace('%s',''))
                if object_['id'] not in objects_dict.keys():
                    objects_dict[object_['id']] = object_
            except Exception as exp:
                try:
                    object_ = json.loads(item)
                    if object_['id'] not in objects_dict.keys():
                        objects_dict[object_['id']] = object_
                except Exception as exp:
                    try:
                        object_ = json.load(item)
                        if object_['id'] not in objects_dict.keys():
                            objects_dict[object_['id']] = object_
                    except Exception as exp:
                        handleException(exp,object_,__name__)
    return objects_dict

def extractResponseContentsFromDict(items=None, objects_dict=dict()):
    if items:
        for item in items:
            try:
                if 'id' in item.keys():
                    if item['id'] not in objects_dict.keys():
                        objects_dict[item['id']] = item
            except Exception as exp:
                handleException(exp,item,__name__)
    return objects_dict

def getTweetContent(object_, original, users_dict, places_dict, media_dict):
    tweet_ = dict()
    
    author_id = object_['author_id']
    public_metrics = object_['public_metrics']
    author_location = 'not_available'
    verified = ''
    protected = ''
    user_name = ''
    user_screen_name = ''
    users_description = ''
    users_followers_count = None
    users_friends_count = None
    place_country = ''
    place_full_name = ''
    location_gps = None
    if 'geo' in object_.keys():
        if 'place_id' in object_['geo'].keys():
            place_id = object_['geo']['place_id']
            if place_id in places_dict.keys():
                if 'full_name' in places_dict[place_id].keys():
                    place_full_name = places_dict[place_id]['full_name']
                if 'country' in places_dict[place_id].keys():
                    place_country = places_dict[place_id]['country']

    if place_full_name == "" and place_country == "":
        location_gps = 'not_available'
    
    if  author_id in users_dict.keys():
        user_obj =  users_dict[author_id]
        if 'location' in user_obj.keys():
            author_location = user_obj['location']
        if 'verified' in user_obj.keys():
            verified = user_obj['verified']
        if 'protected' in user_obj.keys():
            protected = user_obj['protected']  
        if 'screen_name' in user_obj.keys():
            user_screen_name = user_obj['screen_name']
        elif 'username' in user_obj.keys():
            user_screen_name = user_obj['username']
        if 'name' in user_obj.keys():
            user_name = user_obj['name']
        if 'description' in user_obj.keys():
            users_description = user_obj['description']
        if 'public_metrics' in user_obj.keys():
            if 'followers_count' in user_obj['public_metrics'].keys():
                users_followers_count = user_obj['public_metrics']['followers_count']
        if 'public_metrics' in user_obj.keys():
            if 'following_count' in user_obj['public_metrics'].keys():
                users_friends_count = user_obj['public_metrics']['following_count']
        
    media_keys = []
    if "attachments" in object_.keys():
        if "media_keys" in object_["attachments"].keys():
            media_keys = getMediaFromObject(media_dict,object_["attachments"]["media_keys"])
    
    urls = get_urls_from_object(object_)

    tweet_ = {'id':object_['id'],
              'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ')), 
              'created_at_time':time.strftime('%Y-%m-%dT%H:%M:%S', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ')), 
              'created_at_months':time.strftime('%Y-%m', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ')), 
              'created_at_years':time.strftime('%Y', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ')), 
              'emotion': getEmotion(object_['text']) if 'text' in object_.keys() else getEmotion(),
              'favorite_count': public_metrics['like_count'],
              'full_text': object_['text'],
              'hashtags':[x.replace('#','') for x in tweet_tokenizer.tokenize(object_['text']) if x.startswith('#')],
              'mentions':[x.replace('@','') for x in tweet_tokenizer.tokenize(object_['text']) if x.startswith('@')], 
              'language_twitter': language_dict[object_['lang']] if object_['lang'] in language_dict.keys() else object_['lang'],
              'language': get_language(object_['text']),
              'possibly_sensitive': object_['possibly_sensitive'],
              'place_country': place_country,
              'place_full_name': place_full_name,
              'user_id':author_id,
              'location_gps': location_gps,
              'user_location_original': author_location,
              'media_ss': media_keys,
              'platform': getPlatform(object_['source']) if 'source' in object_.keys() else getPlatform(),
              'original_b':original,
              'quote_count_i': public_metrics['quote_count'],
              'retweet_count': public_metrics['retweet_count'],
              'reply_count': public_metrics['reply_count'],
              'urls': urls,
              'verified': verified,
              'protected': protected,
              'conversation_id': object_['conversation_id'] if 'conversation_id' in object_.keys() else None,
              'user_screen_name': user_screen_name,
              'user_name': user_name,
              'users_description': users_description,
              'users_followers_count': users_followers_count,
              'users_friends_count': users_friends_count,
              'matchingRule': object_['matching_rules'] if 'matching_rules' in object_.keys() else None
             }
    
    return tweet_

def extractTweets(file_name, tweets_dict = dict(), original = False, users_dict = dict(), places_dict = dict(), retweets_dict = dict(), replies_dict = dict(), quotes_dict = dict(), media_dict = dict()):
    with open(file_name, 'r', encoding='utf-8') as f:
        for item in f:
            object_ = None
            try:
                object_ = json.loads(item)
            except json.JSONDecodeError as exp1:
                handleException(exp1,object_,f'{__name__} 1')
            except Exception as exp2:
                try:
                    object_ = json.load(item)
                    handleException(exp2,f'{object_}\nBut it was recovered',func_=f'{__name__} 2')
                except Exception as exp3:
                    handleException(exp3,object_,func_=f'{__name__} 3')
            
            if object_: 
                
                if 'referenced_tweets' in object_.keys():
                    
                    for referenced_tweet in object_['referenced_tweets']:
                        
                        if referenced_tweet['type'] == 'retweeted':
                            
                            author_id = object_['author_id']
                            if referenced_tweet['id'] in retweets_dict.keys():
                                if object_['id'] not in retweets_dict[referenced_tweet['id']].keys():
                                    if author_id in users_dict.keys():
                                        user_obj = users_dict[author_id]
                                        if 'screen_name' in user_obj.keys():
                                            retweets_dict[referenced_tweet['id']][object_['id']] = {'user_id' : author_id, 'user_screen_name': user_obj['screen_name'], 'user_name': user_obj['name'], 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}
                                        else:
                                            retweets_dict[referenced_tweet['id']][object_['id']] = {'user_id' : author_id, 'user_screen_name': user_obj['username'], 'user_name': user_obj['name'], 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}
                                        if 'location' in user_obj.keys():
                                            retweets_dict[referenced_tweet['id']][object_['id']]['author_location'] = user_obj['location']
                                    else:
                                        retweets_dict[referenced_tweet['id']][object_['id']] = {'user_id' : author_id, 'user_screen_name': None, 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}
                            else:
                                
                                if author_id in users_dict.keys():
                                    user_obj = users_dict[author_id]
                                    try:
                                        if 'screen_name' in user_obj.keys():
                                            retweets_dict[referenced_tweet['id']] = {object_['id']: {'user_id' : author_id, 'user_screen_name': user_obj['screen_name'], 'user_name': user_obj['name'], 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}}
                                        else:
                                            retweets_dict[referenced_tweet['id']] = {object_['id']: {'user_id' : author_id, 'user_screen_name': user_obj['username'], 'user_name': user_obj['name'], 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}}
                                        if 'location' in user_obj.keys():
                                            retweets_dict[referenced_tweet['id']][object_['id']]['author_location'] = user_obj['location']
                                    except Exception as exp:
                                        handleException(exp,object_,f'{__name__} 4')
                                else:                                
                                    retweets_dict[referenced_tweet['id']] = {object_['id']: {'user_id' : author_id, 'user_screen_name': None}}
                        
                        if referenced_tweet['type'] == 'replied_to':
                            if referenced_tweet['id'] in replies_dict.keys():
                                if object_['id'] not in replies_dict[referenced_tweet['id']].keys():
                                    replies_dict[referenced_tweet['id']][object_['id']] = getTweetContent(object_, original, users_dict, places_dict, media_dict)
                            else:
                                replies_dict[referenced_tweet['id']] = {object_['id']: getTweetContent(object_, original, users_dict, places_dict, media_dict)}
                            replies_dict[referenced_tweet['id']][object_['id']]['in_reply_to_id'] = referenced_tweet['id']
                        
                        if referenced_tweet['type'] == 'quoted':
                            if referenced_tweet['id'] in quotes_dict.keys():
                                if object_['id'] not in quotes_dict[referenced_tweet['id']].keys():
                                    quotes_dict[referenced_tweet['id']][object_['id']] = getTweetContent(object_, original, users_dict, places_dict, media_dict)
                            else:
                                quotes_dict[referenced_tweet['id']] = {object_['id']: getTweetContent(object_, original, users_dict, places_dict, media_dict)}
                            quotes_dict[referenced_tweet['id']][object_['id']]['quotation_id'] = referenced_tweet['id']
                
                else:
                    
                    if object_['id'] not in tweets_dict.keys():
                        
                        tweets_dict[object_['id']] = getTweetContent(object_, original, users_dict, places_dict, media_dict)
                    else:
                        
                        try:
                            if 'retweet_count' in tweets_dict[object_['id']].keys() and 'retweet_count' in object_:
                                tweets_dict[object_['id']]['retweet_count']= max(tweets_dict[object_['id']]['retweet_count'], object_['retweet_count'])
                            if 'reply_count' in tweets_dict[object_['id']].keys() and 'reply_count' in object_:
                                tweets_dict[object_['id']]['reply_count']= max(tweets_dict[object_['id']]['reply_count'], object_['reply_count'])
                            if 'favorite_count' in tweets_dict[object_['id']].keys() and 'like_count' in object_:
                                tweets_dict[object_['id']]['favorite_count']= max(tweets_dict[object_['id']]['like_count'], object_['like_count'])
                            if 'quote_count' in tweets_dict[object_['id']].keys() and 'quote_count' in object_:
                                tweets_dict[object_['id']]['quote_count']= max(tweets_dict[object_['id']]['quote_count'], object_['quote_count'])

                        except Exception as exp:
                            handleException(exp,tweets_dict[object_['id']],f'{__name__} 5')
    return tweets_dict, retweets_dict, replies_dict, quotes_dict


def extractTweetsFromDict(object_, tweets_dict = dict(), original = False, users_dict = dict(), places_dict = dict(), retweets_dict = dict(), replies_dict = dict(), quotes_dict = dict(), media_dict = dict()):
    if object_:
        if type(object_) == list:
            if len(object_) == 1:
                object_ = object_[0]
            else:
                print('Welcome To Facebook!')
                return
        try:
            if 'referenced_tweets' in object_.keys():
                for referenced_tweet in object_['referenced_tweets']:
                    if referenced_tweet['type'] == 'retweeted':
                        author_id = object_['author_id']
                        if referenced_tweet['id'] in retweets_dict.keys():
                            if object_['id'] not in retweets_dict[referenced_tweet['id']].keys():
                                if author_id in users_dict.keys():
                                    user_obj = users_dict[author_id]
                                    if 'screen_name' in user_obj.keys():
                                        retweets_dict[referenced_tweet['id']][object_['id']] = {'user_id' : author_id, 'user_screen_name': user_obj['screen_name'], 'user_name': user_obj['name'], 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}
                                    else:
                                        retweets_dict[referenced_tweet['id']][object_['id']] = {'user_id' : author_id, 'user_screen_name': user_obj['username'], 'user_name': user_obj['name'], 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}
                                    if 'location' in user_obj.keys():
                                        retweets_dict[referenced_tweet['id']][object_['id']]['author_location'] = user_obj['location']
                                else:
                                    retweets_dict[referenced_tweet['id']][object_['id']] = {'user_id' : author_id, 'user_screen_name': None, 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}
                        else:
                            if author_id in users_dict.keys():
                                user_obj = users_dict[author_id]
                                try:
                                    if 'screen_name' in user_obj.keys():
                                        retweets_dict[referenced_tweet['id']] = {object_['id']: {'user_id' : author_id, 'user_screen_name': user_obj['screen_name'], 'user_name': user_obj['name'], 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}}
                                    else:
                                        retweets_dict[referenced_tweet['id']] = {object_['id']: {'user_id' : author_id, 'user_screen_name': user_obj['username'], 'user_name': user_obj['name'], 'created_at':time.strftime('%Y-%m-%d', time.strptime(object_['created_at'],'%Y-%m-%dT%H:%M:%S.%fZ'))}}
                                    if 'location' in user_obj.keys():
                                        retweets_dict[referenced_tweet['id']][object_['id']]['author_location'] = user_obj['location']
                                except Exception as exp:
                                    handleException(exp,object_,f'{__name__} 4')
                            else:
                                retweets_dict[referenced_tweet['id']] = {object_['id']: {'user_id' : author_id, 'user_screen_name': None}}
                    
                    if referenced_tweet['type'] == 'replied_to':
                        if referenced_tweet['id'] in replies_dict.keys():
                            if object_['id'] not in replies_dict[referenced_tweet['id']].keys():
                                replies_dict[referenced_tweet['id']][object_['id']] = getTweetContent(object_, original, users_dict, places_dict, media_dict)
                        else:
                            replies_dict[referenced_tweet['id']] = {object_['id']: getTweetContent(object_, original, users_dict, places_dict, media_dict)}
                        replies_dict[referenced_tweet['id']][object_['id']]['in_reply_to_id'] = referenced_tweet['id']
                    
                    if referenced_tweet['type'] == 'quoted':
                        if referenced_tweet['id'] in quotes_dict.keys():
                            if object_['id'] not in quotes_dict[referenced_tweet['id']].keys():
                                quotes_dict[referenced_tweet['id']][object_['id']] = getTweetContent(object_, original, users_dict, places_dict, media_dict)
                        else:
                            quotes_dict[referenced_tweet['id']] = {object_['id']: getTweetContent(object_, original, users_dict, places_dict, media_dict)}
                        quotes_dict[referenced_tweet['id']][object_['id']]['quotation_id'] = referenced_tweet['id']
            else:
                if object_['id'] not in tweets_dict.keys():
                    tweets_dict[object_['id']] = getTweetContent(object_, original, users_dict, places_dict, media_dict)
                else:
                    try:
                        if 'retweet_count' in tweets_dict[object_['id']].keys() and 'retweet_count' in object_:
                            tweets_dict[object_['id']]['retweet_count']= max(tweets_dict[object_['id']]['retweet_count'], object_['retweet_count'])
                        if 'reply_count' in tweets_dict[object_['id']].keys() and 'reply_count' in object_:
                            tweets_dict[object_['id']]['reply_count']= max(tweets_dict[object_['id']]['reply_count'], object_['reply_count'])
                        if 'favorite_count' in tweets_dict[object_['id']].keys() and 'like_count' in object_:
                            tweets_dict[object_['id']]['favorite_count']= max(tweets_dict[object_['id']]['like_count'], object_['like_count'])
                        if 'quote_count' in tweets_dict[object_['id']].keys() and 'quote_count' in object_:
                            tweets_dict[object_['id']]['quote_count']= max(tweets_dict[object_['id']]['quote_count'], object_['quote_count'])
                    except Exception as exp:
                        handleException(exp,tweets_dict[object_['id']],f'{__name__} 5')
        except Exception as exp3:
            handleException(exp3,object_,func_=f'{__name__}')
    return tweets_dict, retweets_dict, replies_dict, quotes_dict

def extract_raw_responses(json_response, filename = None, OUTPUT_FOLDER=None):
    try:
        tweets = None
        users = None
        includes = None
        places = None
        media = None
        poll = None
        matching_rules = None
        
        if 'matching_rules' in json_response.keys():
            matching_rules = [x['tag'] for x in json_response['matching_rules']]
        
        tweets = json_response['data'] if 'data' in json_response.keys() else None
        if tweets:
            if matching_rules:
                tweets['matching_rules'] = list(set(tweets['matching_rules'] + matching_rules)) if 'matching_rules' in tweets.keys() else list(set(matching_rules)) 
        
        if 'includes' in json_response.keys():
            users = json_response['includes']['users'] if 'users' in json_response['includes'].keys() else None
            includes = json_response['includes']['tweets'] if 'tweets' in json_response['includes'].keys() else None
            if includes:
                for item in includes:
                    if matching_rules:
                        item['matching_rules'] = list(set(item['matching_rules'] + matching_rules)) if 'matching_rules' in item.keys() else list(set(matching_rules))
            
            places = json_response['includes']['places'] if 'places' in json_response['includes'].keys() else None
            media = json_response['includes']['media'] if 'media' in json_response['includes'].keys() else None
            poll = json_response['includes']['poll'] if 'poll' in json_response['includes'].keys() else None
        
        
        if OUTPUT_FOLDER and filename:

            writeDataToFile(tweets, 'tweets_'+filename, OUTPUT_FOLDER)
            writeDataToFile(users, 'users_'+filename, OUTPUT_FOLDER)
            writeDataToFile(includes, 'includes_'+filename, OUTPUT_FOLDER)
            writeDataToFile(places, 'places_'+filename, OUTPUT_FOLDER)
            writeDataToFile(media, 'media_'+filename, OUTPUT_FOLDER)
            writeDataToFile(poll, 'poll_'+filename, OUTPUT_FOLDER)
        else:
            return tweets, users, includes, places, media, poll

    except Exception as exp:
        handleException(exp, object_=json_response, func_=f'\n{__name__}')
