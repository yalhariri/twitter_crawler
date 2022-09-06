import os
import time
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

def job():
    try:
        cmd = "python .util/update_sentiments_eng.py  -c Huawei -u solr_admin -p admin_2018"
        result = os.system(cmd)
        logger.info(f'extracting data done with exit status {result}')
        print(f'extracting data done with exit status {result}')
    except Exception as exp:
        logger.warning(f'extracting data failed with exit status {result}')
        print(f'extracting data failed with exit status {result}')

schedule.every(24).hours.do(job)

while True:
    schedule.run_pending()
    time.sleep(5)