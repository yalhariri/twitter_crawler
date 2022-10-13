import os
import logging
from logging.handlers import TimedRotatingFileHandler as TimedRotatingFileHandler

class WholeIntervalTimedRotatingFileHandler(TimedRotatingFileHandler):
    """A class to manage the backup compression.
    Args:
        TimedRotatingFileHandler ([type]): [description]
    """
    def __init__(self, filename="", when="midnight", interval=1, backupCount=0):
        super(WholeIntervalTimedRotatingFileHandler, self).__init__(
            filename=filename,
            when=when,
            interval=int(interval),
            backupCount=int(backupCount))
    
    def computeRollover(self, currentTime):
        if self.when[0] == 'w' or self.when == 'midnight':
            return super().computeRollover(currentTime)
        return ((currentTime // self.interval) + 1) * self.interval

    def doRollover(self):
        super(WholeIntervalTimedRotatingFileHandler, self).doRollover()
        

log_folder = './../.log/'
status_file = "../.cache/status_streamer"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)
logger = logging.getLogger(__name__)
filename = log_folder + __name__  + '.log'
file_handler = WholeIntervalTimedRotatingFileHandler(filename=filename, when='midnight', interval=1, backupCount=30)#when midnight, s (seconds), M (minutes)... etc
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)



import time
def job():
    try:
        cmd = f"python ./.util/twitter_streamer.py -c  ../.config/.configs.yml -cmd stream -sr False -sf {status_file}"
        result = os.system(cmd)
        if result == 2:
            logger.info(f'streamer done with no exception. Exit status {result}')
            
        print(f'Streamer done with exit status {result}')
        return False
    except KeyboardInterrupt:
        return False
    except Exception as exp:
        logger.warning(f'streamer failed with exit status {result}')
        print(f'streamer failed with exit status {result}')
        return True
    return True

running = True

while running:
    try:
        running = job()
        time.sleep(5)
    except KeyboardInterrupt:
        break
    except Exception as exp:
        pass