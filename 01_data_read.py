# Databricks notebook source
# MAGIC %md
# MAGIC # TwitterStream to S3 (in this case DBFS)

# COMMAND ----------

# DBTITLE 1,Getting Thwitther Credentials
# consumer_key = dbutils.secrets.get("twitter_cred", "apikey")
# consumer_secret = dbutils.secrets.get("twitter_cred", "apisecret")
# access_token = dbutils.secrets.get("twitter_cred", "accesstoken")
# access_token_secret = dbutils.secrets.get("twitter_cred", "accesstokensecret")
# bearer_token = dbutils.secrets.get("twitter_cred", "bearertoken")
consumer_key = "CUQ7ADRjVqkIib4Uws9BZvRCW"
consumer_secret = "CFTO4XiiNsfHXdeJwYj4am4vcriexbSZ5HYeH70npE9GYfNTAH"
access_token = "1666385465614925829-SXZCUx9jzu3pyElcwENAzDCW0ATisv"
access_token_secret = "jDmDUKV3zAZlGVtlZaP2kdUgEVUuDM4MWPzSidD30Y8Pj"

# COMMAND ----------

!pip install tweepy --upgrade

# COMMAND ----------

# DBTITLE 1,Installing Dependencies
!pip install tweepy jsonpickle

# COMMAND ----------

!pip show tweepy

# COMMAND ----------

import tweepy

streaming_client = tweepy.StreamingClient(bearer_token)
streaming_client

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import tweepy
import calendar
import time
import jsonpickle
import sys
import json

# COMMAND ----------

# DBTITLE 1,Authentication to Twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# COMMAND ----------

api = tweepy.API(auth, wait_on_rate_limit=True, timeout=60)
print(f'Twitter screen name: {api.verify_credentials().screen_name}')

# COMMAND ----------

# DBTITLE 1,Subclass Stream
# class TweetStream(tweepy.Stream):
#     def __init__(self, filename):
#         tweepy.Stream.__init__(self, consumer_key=consumer_key, consumer_secret=consumer_secret,
#                                access_token=access_token, access_token_secret=access_token_secret)
        
#         self.filename = filename
#         self.text_count = 0
#         self.tweet_stack = []
#     def on_status(elf, status):
#         #print('*'+status.text)
#         self.text_count = self.text_count + 1
#         self.tweet_stack.append(status)

#         # when to print
#         if (self.text_count % 1 == 0):
#             print(f'retrieving tweet {self.text_count}: {status.text}')
        
#         # how many tweets to batch into one file
#         if (self.text_count % 3 == 0):
#             self.write_file()
#             self.tweet_stack = []

#         # hard exit after collecting n tweets
#         if (self.text_count == 30):
#             raise Exception("Finished job")
#     def write_file(self):
#         file_timestamp = calendar.timegm(time.gmttime())
#         fname = self.filename + '/tweets_' + str(file_timestamp) + '.json'

#         f = open(fname, 'w')
#         for tweet in self.tweet_stack:
#             f.write(jsonpickle.encode(tweet._json, unpickable=False) + '\n')
#         f.close()
#         print("Wrote local file ", fname)

#     def on_error(self, status_code):
#         print("Error with code ", status_code)
#         sys.exit()

# COMMAND ----------

class TweetStream(tweepy.StreamingClient):
    def __init__(self, filename):
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        self.filename = filename
        self.text_count = 0
        self.tweet_stack = []
        super().__init__(auth) # Initialize StreamingClient
 
    def on_data(self, data):
        status = json.loads(data)
        self.text_count = self.text_count + 1
        self.tweet_stack.append(status)

        # when to print
        if (self.text_count % 1 == 0):
            print(f'retrieving tweet {self.text_count}: {status["text"]}')

        # how many tweets to batch into one file
        if (self.text_count % 3 == 0):
            self.write_file()
            self.tweet_stack = []

        # hard exit after collecting n tweets
        if (self.text_count == 30):
            raise Exception("Finished job")

    def write_file(self):
        file_timestamp = calendar.timegm(time.gmtime())
        fname = self.filename + '/tweets_' + str(file_timestamp) + '.json'

        f = open(fname, 'w')
        for tweet in self.tweet_stack:
            f.write(json.dumps(tweet) + '\n')
        f.close()
        print("Wrote local file ", fname)

    def on_error(self, status_code):
        print("Error with code ", status_code)
        sys.exit()

# COMMAND ----------

# DBTITLE 1,Initialize instance of the subclass
tweet_stream = TweetStream("/dbfs/data/twitter_dais2022")

# COMMAND ----------

# DBTITLE 1,Filter Realtime Tweets by keyword
try:
    tweet_stream.filter(lang=["en","de","es"],
                        track=["Data AI World Tour", "Databricks", "DLT"
                               "Delta Live Tables", "ML", "data", "Databricks Workflows"])
except Exception as e:
    print(f"An error occurred: {e}")
