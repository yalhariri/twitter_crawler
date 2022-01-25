# Twitter Crawler based on Twitter API V2

You need to create a yaml file with the following path and name: 
```
../.config/.config.yml 
```

or you can set a file name but you need to add the parameter -c filename when running the code.

### This file should contain the basic information including:


  1- BEARER_TOKEN the Twitter API bearer token.

  2- OUTPUT_FOLDER the folder to write the data to

  3- START_DATE the tweet's starting date (YYYY/M/D)

  4- END_DATE the tweet's last date (YYYY/M/D)

  5- WAIT_TIME waiting time between operations.

  6- LOG path to log file

  7- QUERY, query terms


# To run the commands:

## 1) Search for tweets that contain terms/tokens with the configuration from filename.yml:
```
  python twitter_searcher.py -cmd search -c filename.yml
```

## 2) Extract information from tweets to files:

   ### - Extract to csv file:

  ```
  python twitter_searcher.py -cmd extract_info -tw tweets_file -us users_file  -in incldues_file -ty csv
  ```

   ### Or, (csv is the default file type):

  ```
  python twitter_searcher.py -cmd extract_info -tw tweets_file -us users_file  -in incldues_file
  ```

   ### - Extract to JSON file:
  ```
  python twitter_searcher.py -cmd extract_info -tw tweets_file -us users_file  -in incldues_file -ty json
  ```
  
