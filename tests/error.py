import tweepy
import json
import csv
import lithops
from lithops import Storage,storage
from ast import literal_eval

stg = Storage()
initial = stg.get_object('analysis.data', 'processeddata/sentimentsWithText.csv').decode()
initial = str(initial).splitlines()

finalCSV = ""
for line in initial:
    split = line.split(",",3)
    count = split[3].count(",")
    split[3] = split[3].replace(",","",count-1).replace("\n","")

    finalCSV += ",".join(split)+"\n"

print("UPLOADING")

stg.put_cloudobject(finalCSV, 'analysis.data', 'processeddata/sentimentsWithText.csv')