import tweepy
import json
import csv
from lithops import Storage,storage

def main(args):
    status = 0
    config = {
        'lithops': {'storage': 'ibm_cos', 'storage_bucket': 'analysis.data'},
        'serverless': {'backend': 'ibm_cf'},
        'ibm_cf': {'endpoint': 'https://us-south.functions.cloud.ibm.com', 'namespace': 'david.gaseni@estudiants.urv.cat_dev', 'api_key': '0eb6aaa1-cbfa-4a40-ba3f-8dce99c0d50f:KJaG8bSt2o1LDVCC4tdMhMsudV4ECV2FYgz3w7bjKS2nuOzKZNRBJDJgTBaTvzjz'},
        'ibm_cos': {'endpoint': 'https://s3.eu-gb.cloud-object-storage.appdomain.cloud', 'private_endpoint': 'https://s3.private.eu-gb.cloud-object-storage.appdomain.cloud', 'access_key': 'eb93f6c095ec4c08b817ab1b8434482b', 'secret_key': 'b2c10ef5e2f2ffa5fdf4b1512ae5fd4b83ae2489c6d82149'}
    }

    auth = tweepy.OAuthHandler("PAUnJlWmtF3bbmOgZ8V52PjKy", "6ZUU0hv6FgbcEfjfi8UJNNUwOHJPWd2bHnallsJ6KHVHNeEEq0")
    auth.set_access_token("2387833430-O9jlFNZBnyTMZ3lc6M1tYORglvE8pvQQ5SvsGMI", "5JWcboBlHdIZdcR2P6r7zc9ihnwd2hEgf03fsm8xyX3AF")

    api = tweepy.API(auth, wait_on_rate_limit=True)

    tweet = {}

    # Tweet crawling
    ## Line counter

    stg = Storage(config=config)
    try:
        checkPoint = stg.get_object('analysis.data', 'config/counting.txt').decode()
    except storage.utils.StorageNoSuchKeyError:
        checkPoint = "0,0"

    divided = checkPoint.split(',')

    # Full tweet output
    line = int(divided[0])            # Last read line
    savedTweets = int(divided[1])     # Number of tweets saved

    currentLine = 0
    notCalibrated = True
    secondFile = False

    try:
        if(line < 50079):
            csv_reader = stg.get_object('analysis.data', 'rawdata/datasets/tweetid1.csv').decode()
        else:
            line -= 50079
            secondFile = True
            csv_reader = stg.get_object('analysis.data', 'rawdata/datasets/tweetid2.csv').decode()
        csv_reader = csv_reader.split("\n")
        for row in csv_reader:
            print(row)
            row = row.split(",")
            if notCalibrated and currentLine < line:    # Pass already processed tweets
                currentLine += 1
                if currentLine == line:
                    notCalibrated=False
                continue

            if len(row) != 0:                           # Format tweet
                try:
                    id = row[0]
                    date = row[1]                       # yyyy-MM-dd
                    status = api.get_status(int(id), tweet_mode="extended")
                    location = status.user.location
                    try:
                        text = status.retweeted_status.full_text
                        isRetweet = text != None
                    except AttributeError:  # Not a Retweet
                        text = status.full_text
                        isRetweet = False

                    # Tweet with json format
                    tweet["id"] = id
                    tweet["date"] = date
                    tweet["retweet"] = int(isRetweet)
                    tweet["location"] = location
                    tweet["text"] = text
                    
                    stg.put_cloudobject(str(tweet), 'analysis.data', 'rawdata/tweets/tweet'+str(savedTweets)+'.json')
                    savedTweets+=1
                except tweepy.TweepError:               # Removed tweet
                    pass

            line += 1
            if(not secondFile):
                stg.put_cloudobject(str(line)+','+str(savedTweets), 'analysis.data', 'config/counting.txt')
            else:
                stg.put_cloudobject(str(line+50079)+','+str(savedTweets), 'analysis.data', 'config/counting.txt')
    except storage.utils.StorageNoSuchKeyError:
        status = 1

    return {
        'errorStatus': status
    }