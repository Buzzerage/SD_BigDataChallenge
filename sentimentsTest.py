import json
import gzip
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

sentences = ["Vader is bad"]
analyzer = SentimentIntensityAnalyzer()
for sentence in sentences:
    vs = analyzer.polarity_scores(sentence)
    print(str(vs))

'''g = gzip.open("reviews_Automotive_5.json.gz", 'r')
for l in g:
    review = json.loads(l)
    print(review['overall'])'''