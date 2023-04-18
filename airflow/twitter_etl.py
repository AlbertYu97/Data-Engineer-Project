import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():

  access_key = "RTFEWQr3vk80ylYBE12b6gySk"
  access_secret = "lMnEkXyjk4zLKT4YosmG7RQT2BwHAeRKR8YtppSkrksMz9ePP7"
  consumer_key = "965091732957384704-0kHeBZpLP1of1e5EsrG6PmLZ6UhShoC"
  consumer_secret = "X4ntIrGmpvaKSpwcAriGe8GFdliByNu3QfwDAhlc3Hcry"

  # Twitter authentication
  auth = tweepy.OAuthHandler(access_key, access_secret)
  auth.set_access_token(consumer_key, consumer_secret)

  # Creating an API object
  api = tweepy.API(auth)

  tweets = api.user_timeline(screen_name='@elonmusk',
                            # 200 is the max
                            count=200,
                            # Re_tweet
                            include_rts = False,
                            # Necessray to keey full_text
                            # O/w only first 140 words
                            tweet_mode = 'extended'
                            )

  tweet_list = []

  for tweet in tweets:
      text = tweet._json["full_text"]
      
      refined_tweet = {"user": tweet.user.screen_name,
                      "text": text,
                      "favorite_count": tweet.favorite_count,
                      "retweet_count": tweet.retweet_count,
                      "created_at": tweet.created_at}
      
      tweet_list.append(refined_tweet)

  df = pd.DataFrame(tweet_list)
  df.to_csv("s3://albert-yu-test-bucket/elonmusk_twitter.csv")

