#!/usr/bin/python

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time 
import sys


with open('keys.txt', 'r') as f:
    keys = list(map(lambda x: x.split(' = ')[1], f.readlines()))
    ckey = keys[0]
    csecret = keys[1]
    atoken = keys[2]
    asecret = keys[3]

count = 0
filenamevar = 1


class listener(StreamListener):
    def on_data(self,data):
        global count 
        global filenamevar
        count += 1 
        try:   
            if (count <=1000): #1000 means that after every 1000 tweets, another new CSV file will be created. TODO: checn this to 300,000 later
                filename = str(filenamevar) + ".csv"
                # saveFile = open('twitDB.csv','a')
                saveFile = open(filename,'a')
                saveFile.write(data)
                saveFile.close()
            else: 
                filenamevar +=1 
                count = 0

            return True
        except BaseException,e:
            print ('failed ondata, ',str(e))
            time.sleep(5)

    def on_error(self,status):
        print (status)

auth = OAuthHandler(ckey,csecret)
auth.set_access_token(atoken,asecret)
twitterStream = Stream(auth,listener()) #this line calls the on_data method
twitterStream.filter(locations=[-180,-90,180,90]) # any locatoin in the world. the only filtering you'll get is that the user must not have turned the geotagging off.


# twitterStream.firehose(0) #any tweet but we need to raise permission for this. 
# twitterStream.sample() #random sampel whic is a subset of firehose. this is a good alternative.
# twitterStream.filter(track=["car"]) #filter by keyword

 
        