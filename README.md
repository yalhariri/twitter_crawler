# Twitter Crawler based on Twitter API V2

You need to create ../.config/.config.yml file that contains the basic information including:

1- BEARER_TOKEN the Twitter API bearer token.

2- OUTPUT_FOLDER the folder to write the data to

3- START_DATE the tweet's starting date (YYYY/M/D)

4- END_DATE the tweet's last date (YYYY/M/D)

5- WAIT_TIME waiting time between operations.

6- LOG path to log file

7- QUERY, query terms


To run the command:

1) Search for tweets that contain terms/tokens:
```
  python twitter_searcher.py -cmd search
```

