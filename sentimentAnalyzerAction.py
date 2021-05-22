from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from lithops import Storage
import json
import time
import lithops
from lithops.multiprocessing import Pool

def analyzer(text):
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(text['reviewText'])
    return(str(vs))

def main(args):
    config = {
        'lithops': {'storage': 'ibm_cos', 'storage_bucket': 'analysis.data'},
        'serverless': {'backend': 'ibm_cf'},
        'ibm_cf': {'endpoint': 'https://us-south.functions.cloud.ibm.com', 'namespace': 'david.gaseni@estudiants.urv.cat_dev', 'api_key': '0eb6aaa1-cbfa-4a40-ba3f-8dce99c0d50f:KJaG8bSt2o1LDVCC4tdMhMsudV4ECV2FYgz3w7bjKS2nuOzKZNRBJDJgTBaTvzjz'},
        'ibm_cos': {'endpoint': 'https://s3.eu-gb.cloud-object-storage.appdomain.cloud', 'private_endpoint': 'https://s3.private.eu-gb.cloud-object-storage.appdomain.cloud', 'access_key': 'eb93f6c095ec4c08b817ab1b8434482b', 'secret_key': 'b2c10ef5e2f2ffa5fdf4b1512ae5fd4b83ae2489c6d82149'}
    }

    storage = Storage(config=config)
    data = storage.get_object('analysis.data', 'rawdata/Automotive_5.json').decode()
    data = data.replace("}{","}\n{").splitlines()
    
    with Pool() as pool:
        result = pool.map(analyzer, data[:3])

    return{
        'value': result
    }   