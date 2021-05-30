from posixpath import split

from lithops.job import partitioner
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
from lithops.multiprocessing import Pool
from ast import literal_eval
from lithops import Storage,storage
import csv

def addColumn(dicc):
    data = dicc['res']
    column = dicc['col']
    out = ''
    for i in range(len(data)):
        out += data[i][:-1]+','+str(column[i])+'\n'
    return out

def flat_map(f, xs):
    ys = []
    for x in xs:
        ys.extend(f(x))
    return ys

def dataProcessing(data):
    sentiment = []
    analyzer = SentimentIntensityAnalyzer()

    for d in data:
        try:
            textData = d.split(',')[-1]
            vs = analyzer.polarity_scores(textData)
            if vs == None:
                sentiment.append(0.0)
            else:
                sentiment.append(literal_eval(str(vs))['compound'])
        except IndexError:
            pass

    return sentiment

if __name__ == '__main__':
    stg = Storage()
    try:
        data = stg.get_object('analysis.data', 'processeddata/sentimentsWithText.csv').decode()
        data = str(data).splitlines()

        print(len(data))
        data_length = len(data)         # Number of tweets
        partitions = 10
        incr = data_length // partitions
        result = []
        dicc_result = []

        j = 0
        with Pool() as pool:
            for i in range(1, data_length, incr):
                dicc_result = data[j:i]
                result.append(dicc_result)
                j = i
            if (data_length%incr != 0):       # Include remains
                result.append(data[data_length-data_length%incr:])

            p = pool.map(dataProcessing, result)
            p = flat_map(lambda x: x, p)

            for i in range(1,len(p)):
                data[i] = data[i][:-1]+','+str(p[i])+'\n'

        data[0] = "Id,Date,Retweet,location,Text,Sentiment\n"
        data = "".join(data)
        stg.put_cloudobject(data, 'analysis.data', 'processeddata/sentimentsData.csv')
    except storage.utils.StorageNoSuchKeyError:
            exit()