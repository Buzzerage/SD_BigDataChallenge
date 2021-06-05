import tweepy
import json
import csv
import lithops
from lithops import Storage,storage
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from ast import literal_eval

def analyzer(text):
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(text)
    sentiment = literal_eval(str(vs))['compound']
    return(sentiment)

def main(args):
    status = 0
    
    '''
    API credentials and runtime config to be specified.
    '''

    api = tweepy.API(auth, wait_on_rate_limit=True)

    tweet = {}

    # Tweet crawling
    ## Line counter

    stg = Storage(config=config)
    try:
        checkPoint = stg.get_object('analysis.data', 'config/counting2.txt').decode()
    except storage.utils.StorageNoSuchKeyError:
        checkPoint = "0,0"

    divided = checkPoint.split(',')

    # Full tweet output
    line = int(divided[0])            # Last read line
    savedTweets = int(divided[1])     # Number of tweets saved

    currentLine = 0
    notCalibrated = True
    secondFile = False

    fexec = lithops.FunctionExecutor()
    try:
        finalCSV = stg.get_object('analysis.data', 'processeddata/sentiments2.csv').decode()
    except storage.utils.StorageNoSuchKeyError:
        finalCSV = ""

    try:
        csv_reader = stg.get_object('analysis.data', 'rawdata/datasets/englishTweetsLarge.csv').decode()

        csv_reader = csv_reader.split("\n")
        for row in csv_reader:
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

                    # CSV format
                    sentiment = analyzer(text)
                    finalCSV += str(id)+","+str(date)+","+str(isRetweet)+","+str(location)+","+str(sentiment)+"\n"
                    savedTweets+=1

                    if((savedTweets % 300) == 0):
                        stg.put_cloudobject(finalCSV, 'analysis.data', 'processeddata/sentiments2.csv')
                        stg.put_cloudobject(str(line+1)+','+str(savedTweets), 'analysis.data', 'config/counting2.txt')

                except tweepy.TweepError:               # Removed tweet
                    pass

            line += 1
    except storage.utils.StorageNoSuchKeyError:
        status = 1

    return {
        'errorStatus': status
    }