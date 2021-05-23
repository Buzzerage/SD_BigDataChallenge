from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import lithops
from ast import literal_eval
from lithops.multiprocessing import Pool
from lithops import Storage,storage

def analyzer(data):
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(data['text'])
    return(str(vs))

def main(args):
    config = {
        'lithops': {'storage': 'ibm_cos', 'storage_bucket': 'analysis.data'},
        'serverless': {'backend': 'ibm_cf', 'runtime': 'buzzerage/sd_sentiment_analysis:latest'},
        'ibm_cf': {'endpoint': 'https://us-south.functions.cloud.ibm.com', 'namespace': 'david.gaseni@estudiants.urv.cat_dev', 'api_key': '0eb6aaa1-cbfa-4a40-ba3f-8dce99c0d50f:KJaG8bSt2o1LDVCC4tdMhMsudV4ECV2FYgz3w7bjKS2nuOzKZNRBJDJgTBaTvzjz'},
        'ibm_cos': {'endpoint': 'https://s3.eu-gb.cloud-object-storage.appdomain.cloud', 'private_endpoint': 'https://s3.private.eu-gb.cloud-object-storage.appdomain.cloud', 'access_key': 'eb93f6c095ec4c08b817ab1b8434482b', 'secret_key': 'b2c10ef5e2f2ffa5fdf4b1512ae5fd4b83ae2489c6d82149'}
    }
    stg = Storage(config=config)
    try:
        index = int(stg.get_object('analysis.data', 'config/analyzedDataIndex.txt').decode())
    except storage.utils.StorageNoSuchKeyError:
        index = 0

    while True:
        try:
            data = stg.get_object('analysis.data', 'rawdata/tweets/tweet'+str(index)+'.json').decode()
            data = literal_eval(data)
            sentiment = literal_eval(analyzer(data))['compound']
            tweet = {}
            tweet["id"] = data['id']
            tweet["date"] = data['date']
            tweet["retweet"] = data['retweet']
            tweet["location"] = data['location']
            tweet["sentiment"] = sentiment
            index += 1

            stg.put_cloudobject(json.dumps(tweet), 'analysis.data', 'analysisdata/tweets/'+tweet["date"]+'/tweet'+str(index)+'.json')
            stg.put_cloudobject(index, 'analysis.data', 'config/analyzedDataIndex.txt')
        except storage.utils.StorageNoSuchKeyError:
            break
    return{
        'value': ''
    }   
    
print(main(2))