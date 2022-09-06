import os
import time
import pandas as pd
import logging
import schedule
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
        super(TimedRotatingFileHandler, self).doRollover()
        
log_folder = './../.log/'
if not os.path.exists(log_folder):
    os.makedirs(log_folder)
logger = logging.getLogger(__name__)
filename = log_folder + __name__  + '.log'
file_handler = TimedRotatingFileHandler(filename=filename, when='midnight', interval=1, backupCount=30)#when midnight, s (seconds), M (minutes)... etc
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

tweets_file = '/media/data/huawei-absa/data/Tweets/tweets.csv'
aspect_file = '/media/data/huawei-absa/outputs/tweet-aspects.csv'
def job():
    try:

        cmd = f"python .util/get_no_aspects.py  -c Huawei -u solr_admin -p admin_2018 -tf {tweets_file}"
        result = os.system(cmd)
        logger.info(f'extracting no aspect data done with exit status {result}')
        print(f'extracting no aspect data done with exit status {result}')
        
        if os.path.exists(tweets_file):
            tweets_df = pd.read_csv(tweets_file)
            if len(tweets_df[tweets_df['language']=='english']) > 100 and len(tweets_df[tweets_df['language']!='english']) > 100:
                os.chdir('/media/data/huawei-absa/')
                cmd = "./run.sh"
                result = os.system(cmd)
                logger.info(f'extracting data done with exit status {result}')
                print(f'extracting data done with exit status {result}')
            else:
                print('tweets file found, but data size is not enough... waiting for next running time!')
        else:
            print('no tweets file found')
        os.chdir('/media/data/youssef/main_work/twitter_crawler_v2')
        cmd = f"python .util/update_aspects.py  -c Huawei -u solr_admin -p admin_2018  -af {aspect_file}"
        result = os.system(cmd)
        logger.info(f'extracting data done with exit status {result}')
        print(f'extracting data done with exit status {result}')
        
    except Exception as exp:
        print(exp)
        logger.warning(f'extracting data failed with exit status {result}')
        print(f'extracting data failed with exit status {result}')

schedule.every(12).hours.do(job)

while True:
    schedule.run_pending()
    time.sleep(5)