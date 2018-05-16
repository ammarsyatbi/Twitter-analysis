#this script will save from stream tweets directly into database
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import sqlite3
import sys

conn = sqlite3.connect('streamingscrap.sqlite')
conn.text_factory = str
cur = conn.cursor()

#Get upcoming tweets

#consumer key, consumer secret, access token, access secret.
# User your own keys
ckey="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
csecret="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
atoken="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
asecret="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

#cur.execute('''DROP TABLE IF EXISTS Tweets ''')
cur.execute('''CREATE TABLE IF NOT EXISTS Tweets
    (id INTEGER PRIMARY KEY, tweet TEXT, username Text, language Text, location Text, twDate Text)''')

class listener(StreamListener):

    def on_data(self, data):

        all_data = json.loads(data)
        tweet = all_data["text"]
        username = all_data["user"]["screen_name"]
        language = all_data["lang"]
        location = all_data["user"]["location"]
        twDate = all_data['created_at']

        cur.execute('INSERT OR IGNORE INTO Tweets (tweet, username, language, location, twDate) VALUES (?, ?, ?, ?, ?)', (tweet, username, language, location, twDate) )

        conn.commit()

        print((username,tweet, language, location, twDate))

        return True

    def on_error(self, status):
        print(status)
        if status == 420:
            # returning False in on_data disconnects the stream
            return False

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
api = tweepy.API(auth)

#screen_name = 'MarStebi'

# Get user id
# nama = 'NST_Online'
# user = api.get_user(screen_name = nama)
# user_id = str(user.id)

twitterStream = Stream(auth, listener())

# Scrap by user id
#twitterStream.filter(follow=[user_id])
twitterStream.filter(track=['Tabung Haji','Theta Edge','Theta Mobile'])
cur.close()
