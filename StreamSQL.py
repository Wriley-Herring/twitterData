import mysql.connector
from mysql.connector import Error
import tweepy
import json
from dateutil import parser
import time
import os
import subprocess

#importing file which sets env variable
subprocess.call("./settings.sh", shell = True)

consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_token_secret = os.environ['ACCESS_TOKEN_SECRET']
password = os.environ['PASSWORD']




def connect(username, created_at, tweet, retweet_count, place, location):
    """
    connect to MySQL database and insert twitter data
    """
    try:
        con = mysql.connector.connect(host = 'localhost',database='twitterDB',user='root',password=password,charset='utf8')

        if con.is_connected():
            """
            Insert twitter data
            """
            cursor = con.cursor()

            #twitter, golf
            query = "INSERT INTO Golf (username, created_at, tweet, retweet_count, place, location) VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(query,(username, created_at, tweet, retweet_count, place, location))
            con.commit()
    except Error as e:
        print(e)

    cursor.close()
    con.close()

    return

class Streamlistener(tweepy.StreamListener):

    def on_connect(self):
        print("You are connected to Twitter API")

    def on_error(self):
        if status_code != 200:
            print('error found')
            #returning false disconnects the stream
            return false 

    """
    This method reads in tweet data as Json
    and extracts the data we want.
    """
    def on_data(self,data):

        try:

            raw_data = json.loads(data)

            if 'text' in raw_data:

                username = raw_data['user']['screen_name']
                created_at = parser.parse(raw_data['created_at'])
                tweet = raw_data['text']
                retweet_count = raw_data['retweet_count']

                if raw_data['place'] is not None:
                    place = raw_data['place']['country']
                    print(place)
                else:
                    place = None

                location = raw_data['user']['location']

                #insert data just collected into MySQL database
                connect(username, created_at, tweet, retweet_count, place, location)
                print("Tweet collected at: {}".format(str(created_at)))

        except Error as e:
            print(e)

if __name__ == '__main__':


    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit = True)

    # create instance of Streamlistener
    listener = Streamlistener(api = api)
    stream = tweepy.Stream(auth,listener = listener)

    track = ['golf','masters','reed','mcilroy','woods']
    stream.filter(track=track, languages = ['en'])


