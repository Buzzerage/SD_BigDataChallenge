import json
import tweepy
import pandas as pd

with open("twitter_keys.json", "r") as file:
    twitter_keys = json.load(file)

auth = tweepy.OAuthHandler(twitter_keys['API_KEY'], twitter_keys['API_SECRET_KEY'])
auth.set_access_token(twitter_keys['ACCESS_TOKEN'], twitter_keys['ACCESS_TOKEN_SECRET'])
api = tweepy.API(auth, wait_on_rate_limit=True)

text_query = 'Pedro Sanchez'
count = 3   # Creation of query object
try:
    # Creation of query method using parameters
    tweets = tweepy.Cursor(api.search,q=text_query, since='2020-01-01', until='2021-05-09', tweet_mode='extended', Lang='es').items(count)

    # Pulling information from tweets iterable object
    tweets_list = [[tweet.created_at, tweet.id, tweet.full_text] for tweet in tweets]
    
    for t in tweets_list:
        print(t)
    # Creation of dataframe from tweets list
    # Add or remove columns as you remove tweet information
    #tweets_df = pd.DataFrame(tweets_list)
    #print(tweets_df)

except BaseException as e:
    print('failed on_status,',str(e))
    time.sleep(3)

