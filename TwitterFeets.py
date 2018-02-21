#! /usr/bin/python

from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
from progressbar import ProgressBar, Percentage, Bar
import json
import sys
import csv
import subprocess

#Twitter app information
consumer_secret=''
consumer_key=''
access_token=''
access_token_secret=''

#Create the listener class that receives and saves tweets
class listener(StreamListener):
    def on_data(self, data):
        #Append the tweet to the 'tweets.txt' file
        with open('tweets', 'a') as tweet_file:
            all_data = json.loads(data)
            text = all_data["text"]
            text = text.encode('ascii', 'ignore')
            text = text.replace('|', ' ')
            text = text.replace('\n', ' ')
            text = text.replace('&', 'and')
            date = all_data["created_at"]
            userlocation = all_data["user"]["location"]
            username = all_data["user"]["screen_name"]
            if not (text is None or date is None or userlocation is None or username is None):
               writer = csv.writer(tweet_file)
               writer.writerow([username.encode("utf-8"),userlocation.encode("utf-8"), text, date])
               print((username,userlocation, text, date))               
            return True
			subprocess.call("hdfs dfs -put -f tweets /twitter", shell=True)
    def on_error(self, status):
        print status
#Get the OAuth token
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
#Use the listener class for stream processing
twitterStream = Stream(auth, listener())
twitterStream.filter(track=["test"])

