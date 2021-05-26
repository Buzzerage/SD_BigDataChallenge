import time
from lithops.multiprocessing import Pool
from lithops import Storage
import json
import time
import lithops
from lithops.multiprocessing import Pool
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from ast import literal_eval

def f(x):
    stg = Storage()
    data = stg.get_object('analysis.data', 'rawdata/datasets/englishTweetsReduced.csv')
    return(data)

if __name__ == '__main__':
    with Pool() as p:
        r = p.map(f,range(3))
        print(str(r))