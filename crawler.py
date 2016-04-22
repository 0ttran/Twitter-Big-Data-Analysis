import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import sys
import argparse

import json
import urllib2
from httplib import BadStatusLine
from lxml.html import parse
import time
import math
import re

#arguments
dirName = str(sys.argv[2]) #data path
numTweets = int(sys.argv[1]) #num of tweets 

#twitter credentials
access_token = "794400235-KdRUNxANgh8hihIXEusBQ1KG56M69CnvbNqlVyZL"
access_token_secret = "TeLtxZiP6rNG1NVFIZ4xlGXRCndr0plGpkBnGnwA2kkSA"
consumer_key = "6qkJiouDjxgANzJHQ5bLZMyiI"
consumer_secret = "gWRU7UlKAKJvUxy5HHPYotp1KrDewyHxpQkyCC55lqCsYQvCV1"

tweetcnt = 0
filecnt = 0
outputPath = dirName
outputPath += '/'
outputPath += 'twitter_data'
outputPath += str(filecnt)
outputPath += '.txt'
f = open(outputPath, 'a')
chkFlag = True
numTweetFlg = 0
jsonData = []
notDone = True

def parseAndStoreData(decoded):
        global f
        global filecnt
        global tweetcnt
        global chkFlag

        #checks num of tweet parameter
        if tweetcnt >= numTweets and numTweets != 0:
            print "first"
            chkFlag = False
            return False

        #Ends when files reach 5GB in total size
        if (filecnt >= 100):
            print "filecnt"
            chkFlag = False
            return False

        #Create a new text file every 50MB
        if (f.tell() >= 50000000):
            print "last"
            f.close()
            chkFlag= True
            filecnt += 1
            outputPath = dirName
            outputPath += '/'
            outputPath += 'twitter_data'
            outputPath += str(filecnt)
            outputPath += '.txt'
            f = open(outputPath, 'a')

        username = unicode(decoded['user']['screen_name']).encode("ascii","ignore") #gets username
        userTweet = unicode(decoded['text']).encode("ascii","ignore") #gets tweet
        userTweet = userTweet.replace('\n', ' ').replace('\t', '').replace('\r', '') #replaces new lines
        userTweetTime = unicode(decoded['created_at']) #gets timestamp
        userLocation = unicode(decoded['user']['location']).encode("ascii","ignore") #gets location as per profile, not of the specific tweet
        userCoords = unicode(decoded['coordinates']).encode("ascii","ignore") #gets coordinates, will be 'None' if they have disable location services
        userURLS = unicode(decoded['entities']['urls']).encode("ascii","ignore") #get URLS 
        userNumFollowers = unicode(decoded['user']['followers_count']).encode("ascii","ignore") #get number of followers the user has
        userData = ("Date:" + userTweetTime +  
                    " UserLocation:" + userLocation +
                    " Coords:" + userCoords[36:-1] +
                    " User:" + username + 
                    " Text:" + userTweet + 
                    " NumFollwers:" + userNumFollowers)

           
        userData += " Hashtags:"
        #Loops through the list of hashtags and adds them to userData
        userHashtags = decoded['entities']['hashtags']
        if (userHashtags != "[]"):
            tmp = decoded['text']
            for Hashtags in userHashtags:
                userHashtags = unicode(Hashtags['text']).encode("ascii","ignore")
                userData += userHashtags + " "
            
        #get urls
        pageTitle = None
        '''
        userData += " URL:"
        if userURLS != "[]":
            expanded_url = unicode(decoded['entities']['urls'][0]['expanded_url']).encode("ascii","ignore")
            userData += expanded_url

            try:
                page = urllib2.urlopen(expanded_url)
                p = parse(page)
                    
                #pageTitle = unicode(p.find(".//title").text).encode("utf-8")
                pageT = p.find(".//title")
                if (pageT != None):
                    pageTitle = unicode(p.find(".//title").text).encode("ascii","ignore")
            except urllib2.HTTPError, err:
                if err.code == 404:
                    print "Page not found!"
                elif err.code == 403:
                    print "Access denied!"
                else:
                    print "Error:", err.code
            except urllib2.URLError, err:
                print "URL error:", err.reason
            except BadStatusLine:
                print "Could not fetch URL"
       
        userData += " Title:"
        if (pageTitle != None):
            #pageTitle.replace('\n', '').replace('\r', ' ').replace('\t', '')
            pageTitle = re.sub('[^A-Za-z0-9]+', ' ', pageTitle)
            userData += pageTitle
        '''
            
        tweetcnt += 1
        print 'Tweet:', tweetcnt, ' F.size = ', f.tell(), ' on file:', filecnt 
        userData += "\n"
        print userData
        f.write(userData)


#twitter listener
class twitterListener(StreamListener):
    
    def on_data(self, data):
        global numTweetFlg
        global jsonData
        global chkFlag
  
        #Get json file that contains information
        decoded = json.loads(data) 
    
        #Checks if tweet is geo-tagged
        if unicode(decoded['user']['geo_enabled']).encode("ascii","ignore") == "True" and unicode(decoded['coordinates']).encode("ascii","ignore") != "None":
            print "Tweet #: " + str(numTweetFlg)
            numTweetFlg = numTweetFlg + 1
            jsonData.append(decoded)

        #Stops at specified number of tweets
        if numTweetFlg == 5000:
            chkFlag = False
            return False

        return True

    def on_error(self, status):
        print status
        chkFlag = False
        if (status == 420):
            print "FOUND 420!!!"
            return False
           


if __name__ == '__main__':
    
    wait_counter = 0
    notDone = True

    #Takes in 1000 tweets at a time and processes them and repeats
    while notDone:
        while chkFlag:
            try:
                #Authentication and connection to twitter API
                l = twitterListener()
                auth = OAuthHandler(consumer_key, consumer_secret)
                auth.set_access_token(access_token, access_token_secret)
                stream = Stream(auth, l)

                stream.filter(locations=[-123.40,35.59,-66.79,48.25], languages=["en"]) 
            except Exception, e:
                print "Exception occured: "
                print e
                if (e == 420):
                    waittime = 60;
                    print "WAITING for " , waittime , " seconds..."
                    time.sleep(waittime)
                    print "Going"
                pass
            
        #Postprocessing here
        for data in jsonData:
            parseAndStoreData(data)
        jsonData = []

    f.close()