import tweepy
import json
import csv
import lithops
from lithops import Storage,storage
from ast import literal_eval

def main(args):
    status = 0
    config = {
        'lithops': {'storage': 'ibm_cos', 'storage_bucket': 'analysis.data'},
        'serverless': {'backend': 'ibm_cf'},
        'ibm_cf': {'endpoint': 'https://us-south.functions.cloud.ibm.com', 'namespace': 'david.gaseni@estudiants.urv.cat_dev', 'api_key': '0eb6aaa1-cbfa-4a40-ba3f-8dce99c0d50f:KJaG8bSt2o1LDVCC4tdMhMsudV4ECV2FYgz3w7bjKS2nuOzKZNRBJDJgTBaTvzjz'},
        'ibm_cos': {'endpoint': 'https://s3.eu-gb.cloud-object-storage.appdomain.cloud', 'private_endpoint': 'https://s3.private.eu-gb.cloud-object-storage.appdomain.cloud', 'access_key': 'eb93f6c095ec4c08b817ab1b8434482b', 'secret_key': 'b2c10ef5e2f2ffa5fdf4b1512ae5fd4b83ae2489c6d82149'}
    }

    auth = tweepy.OAuthHandler("I9IqZZP3UVCOGxCd97IJE2jyf", "TMaGVpIwHQiQUgCAJfxTFmnyJYuTSqMOloO5DZRcmUasYDZCUo")
    auth.set_access_token("1236954932-DAaai7Bhm6tE98RQSzRhHRf30AZ1iXS4TfXdf4z", "vKxDGIwBa6DpmMeJC2fKMFGSIg0OT3gJG03q9DxGFKTiR")

    api = tweepy.API(auth, wait_on_rate_limit=True)

    tweet = {}

    # Tweet crawling
    ## Line counter

    stg = Storage(config=config)
    try:
        checkPoint = stg.get_object('analysis.data', 'config/countingWithText.txt').decode()
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
        finalCSV = stg.get_object('analysis.data', 'processeddata/sentimentsWithText.csv').decode()
    except storage.utils.StorageNoSuchKeyError:
        finalCSV = "Id,Date,Retweet,location,Text"

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
                    finalCSV += str(id)+","+str(date)+","+str(isRetweet)+","+str(location)+','+str(text.replace(",","").replace("\n",""))+"\n"
                    savedTweets+=1
                    print(row)
                    if((savedTweets % 300) == 0):
                        stg.put_cloudobject(finalCSV, 'analysis.data', 'processeddata/sentimentsWithText.csv')
                        stg.put_cloudobject(str(line+1)+','+str(savedTweets), 'analysis.data', 'config/countingWithText.txt')

                except tweepy.TweepError:               # Removed tweet
                    pass

            line += 1
    except storage.utils.StorageNoSuchKeyError:
        status = 1

    return {
        'errorStatus': status
    }
