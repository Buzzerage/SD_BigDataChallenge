import tweepy
import json
import csv

# Tweepy authentication
with open("twitter_credentials.json", "r") as file:
    creds = json.load(file)

auth = tweepy.OAuthHandler(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])
auth.set_access_token(creds['ACCESS_TOKEN'], creds['ACCESS_SECRET'])

api = tweepy.API(auth, wait_on_rate_limit=True)

tweet = {}
# Tweet crawling
## Line counter
countFile = open('counting.txt', mode='r')
try:
    for nline in countFile:
        checkPoint = nline
except ValueError:
    pass
divided=checkPoint.split(',')
countFile.close()

countFile = open('counting.txt', mode='a')
## Full tweet output
line=int(divided[0])            # Last read line
savedTweets=int(divided[1])     # Number of tweets saved
nFile=savedTweets//300          # File to be written
#done=savedTweets%300            # Number of written tweets per file
remaining=300-savedTweets%300   # Remaining tweets to end the file
outputFile = open('jsonTweetListTest%s.tsv' % nFile, 'a')
currentLine = 0
notCalibrated=True
with open('englishTweetsReduced.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        if remaining == 0 :                         # Remaining lines until file size cap
            remaining=300
            outputFile.close()
            outputFile = open('jsonTweetListTest%s.tsv' % nFile, 'a')

        if notCalibrated and currentLine < line:    # Pass already processed tweets
            print(str(currentLine)+"<"+str(line))
            currentLine += 1
            if currentLine == line:
                notCalibrated=False
            continue

        if line % 1000:                             # Clear file counter every 1000
            countFile.truncate(0)
            countFile.write(str(line)+','+str(savedTweets)+'\n')
        line += 1

        if len(row) != 0:                           # Format tweet
            try:
                id = row[0]
                date = row[1]           # yyyy-MM-dd
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
                print(tweet)
                outputFile.write(str(tweet)+"\n")
                savedTweets+=1
                remaining-=1
            except tweepy.TweepError:   # Removed tweet
                pass
        
        
        countFile.write(str(line)+','+str(savedTweets)+'\n')    # Checkpoint

countFile.close()