import tweepy
import json
import csv
from lithops import Storage,storage

def main(args):
    status = 0
    
    '''
    API credentials and runtime config to be specified.
    '''

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