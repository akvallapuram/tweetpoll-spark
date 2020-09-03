from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys
import re
import random
from textblob import TextBlob
import json
import requests
import traceback


# candidate indices for elasticsearch
candidates = ['bennet', 'biden', 'bloomberg', 'booker', 'bullock', 
              'buttigieg', 'castro', 'delaney', 'gabbard', 'harris',
              'kloubchar', 'patrick', 'sanders', 'sestak', 'steyer',
              'warren', 'williamson', 'yang', 'trump', 'walsh', 'weld']


# get text from tweet
def get_text(tweet_row):
    """
    Returns the text from tweet data. 

    Args:
        tweet_row (spark.sql.Row): data for a single tweet 
            loaded from .csv file to a pyspark DataFrame. 
    
    Returns:
        text (string): processed text from the tweet. 
    """
    tweet = None
    if tweet_row.truncated:
        tweet = tweet_row.extended_tweet.full_text
    
    else:
        tweet = tweet_row.text
  
    tweet = re.sub(r'http\S+', '', tweet)
    tweet = re.sub(r'[^\w\s]','', tweet)
    return tweet.lower()


# get_sentiment
def get_sentiment(tweet_row):
    """
    Returns the sentiment score for a tweet. 

    Args:
        tweet_row (spark.sql.Row): data for a single tweet 
            loaded from .csv file to a pyspark DataFrame.
    
    Returns:
        sentiment_score (float): value between -1 and 1. 
    """
    blob = TextBlob(get_text(tweet_row))
    return blob.sentiment.polarity


# get_location
def get_location(tweet_row):
    """
    Returns random latitude and longitude coordinates given the
    geolocation of a tweet as bounding boxes. 

    Args:
        tweet_row (spark.sql.Row): data for a single tweet 
            loaded from .csv file to a pyspark DataFrame.
    
    Returns:
        loc (list): two floating point latitude and longitude coordinates. 
    """
    loc = None
    if tweet_row.coordinates:
        loc = tweet_row.coordinates["coordinates"]
  
    elif tweet_row.place:
        x, y = zip(*tweet_row.place.bounding_box.coordinates[0])
        loc = [random.uniform(min(x), max(x)), random.uniform(min(y), max(y))]
    return loc


def upload_to_elasticsearch(x):
    """
    Uploads the sentiment analysis results of a tweet to elasticsearch
    where each document is indexed by the last name of a presidential 
    candidiate. 

    Args:
        x (dict): The sentiment analysis data to be stored in the following
            format:

            {
                'candidate' (string): lower case last name of the candidate,
                'latitude' (float): latitude coordinate of the tweet, 
                'longitude' (float): longitude coordinate of the tweet,
                'sentiment' (float): sentiment score of the tweet. 
            }
    
    Returns:
        None
    """
    url = 'https://search-tweetpoll-unkjimt57b3xoepobbbgi434m4.us-east-1.es.amazonaws.com/'
    url += x['candidate'] + '/_doc'
    del x['candidate']
    requests.post(url, data=json.dumps(x), headers={'Content-type': 'application/json'})


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            print("correct usage is tweetSentiment <file>", file=sys.stderr)
            exit(-1)
        
        # get context
        sc = SparkContext(appName="tweetpoll")
        sqlContext = SQLContext(sc)
        
        # MapReduce
        tweets_df = sqlContext.read.json(sys.argv[1])
        
        # sentiment analysis
        sentimentsRDD = tweets_df.rdd \
                    .filter(lambda x: x.text != None) \
                    .map(lambda x: (x.id, (get_location(x), get_sentiment(x)))) \
                    .filter(lambda x: x[1][0] != None)

        # retrieve candidates
        candidatesRDD = tweets_df.rdd \
                    .map(lambda x: (x.id, get_text(x).split(' '))) \
                    .flatMapValues(lambda value: value) \
                    .filter(lambda x: x[1] in candidates)

        result = candidatesRDD.join(sentimentsRDD) \
                .map(lambda x: ({'candidate': x[1][0], 'latitude': x[1][1][0][0], 
                                        'longitude':x[1][1][0][1], 'sentiment': x[1][1][1]}))

        # Save to Elasticsearch
        result.map(upload_to_elasticsearch).collect()

    except:
        print(traceback.format_exc(), file=sys.stderr)

    # the end
    sc.stop()