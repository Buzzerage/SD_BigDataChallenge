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
            nonProcessed.append("ee")

    return sentiment


stg = Storage()
try:
    nonProcessed = []
    data = stg.get_object('analysis.data', 'processeddata/sentimentsWithText.csv').decode()
    data = str(data).splitlines()
    #data[0] = flat_map(lambda x: x, data[0].split(',')[4:])
    #data[0] = str.removeprefix(data[0],'Text')
    print(len(data))
    '''
    full_text = []
    for line in data:
        try:
            sepLine = line.split(',')
            full_text.append(sepLine[:-1])
        except IndexError:
            nonProcessed.append(line)
    '''
    '''
    csv_text = csv.reader(data, delimiter=',')
    full_text = []
    for line in  csv_text:
        try:
            full_text.append(line[-1])        # Array of tweet text
        except IndexError:
            nonProcessed.append(line)
    '''
    '''
    print("##################")
    print(len(full_text)+len(nonProcessed))
    data_length = len(full_text)         # Number of tweets
    partitions = 10
    incr = data_length // partitions
    '''
    data_length = len(data)         # Number of tweets
    partitions = 10
    incr = data_length // partitions
    result = []
    dicc_result = []

    j = 0
    with Pool() as pool:
        for i in range(0, data_length, incr):
            dicc_result = data[j:i]
            result.append(dicc_result)
            j = i
        if (data_length%incr != 0):       # Include remains
            result.append(data[data_length-data_length%incr:])
        print("eeeeeee")
        a=0
        for i in result:
            a+=len(i)
        print(a)
        p = pool.map(dataProcessing, result)
        p = flat_map(lambda x: x, p)

        for i in range(len(p)):
            data[i] = data[i][:-1]+','+str(p[i])+'\n'

        #print(data)
    # stg.put_object('analysis.data', data)
except storage.utils.StorageNoSuchKeyError:
        exit()


