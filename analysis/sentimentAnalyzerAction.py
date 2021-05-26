from requests.models import RequestField
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import lithops
from ast import literal_eval
from lithops import Storage,storage

def analyzer(index):
    stg = Storage()
    try:
        data = stg.get_object('analysis.data', 'rawdata/tweets/tweet'+str(index)+'.json').decode()
        data = literal_eval(data)
        analyzer = SentimentIntensityAnalyzer()
        vs = analyzer.polarity_scores(data['text'])
        sentiment = literal_eval(str(vs))['compound']
        csvNewLine = str(data['id'])+","+str(data['date'])+","+str(data['retweet'])+","+data['location'].replace(",","")+","+str(sentiment)+"\n"
        return(csvNewLine)
    except storage.utils.StorageNoSuchKeyError:
        return("")

if __name__ == '__main__':
    stg = Storage()
    
    try:
        index = int(stg.get_object('analysis.data', 'config/analyzedDataIndex.txt'))
    except storage.utils.StorageNoSuchKeyError:
        index = 0

    try:
        csv = stg.get_object('analysis.data', 'processeddata/sentiments.csv').decode()
    except storage.utils.StorageNoSuchKeyError:
        csv = "Id,Date,Retweet,Location,Sentiment\n"

    
    for i in range(index,index+900):
        csv += analyzer(i)
        if((i % 1000) == 0):
            stg.put_cloudobject(csv, 'analysis.data', 'processeddata/sentiments.csv')
            stg.put_cloudobject(str(index+i), 'analysis.data', 'config/analyzedDataIndex.txt')
        print(i)

    stg.put_cloudobject(csv, 'analysis.data', 'processeddata/sentiments.csv')
    stg.put_cloudobject(str(index+900), 'analysis.data', 'config/analyzedDataIndex.txt')
    