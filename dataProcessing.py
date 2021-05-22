from lithops import Storage
import json
import time
import lithops
from lithops.multiprocessing import Pool
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def f(data):
    review = json.loads(data)
    fexec = lithops.FunctionExecutor(runtime='buzzerage/sd_sentiment_analysis:latest')
    fexec.call_async(retrieveSentiments, review['reviewText'])
    return(fexec.get_result())

def retrieveSentiments(text):
    analyzer = SentimentIntensityAnalyzer()
    vs = analyzer.polarity_scores(text)
    return(str(vs))
    
if __name__ == '__main__':
    storage = Storage()
    data = storage.get_object('analysis.data', 'rawdata/Automotive_5.json').decode()
    data = data.replace("}{","}\n{").splitlines()
    with Pool() as pool:
        result = pool.map(f, data[:3])
    print(result)

    '''with lithops.FunctionExecutor() as fexec:
        fexec.call_async(f, None)
        print(fexec.get_result())'''
