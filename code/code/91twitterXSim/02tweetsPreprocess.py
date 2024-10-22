import json
from time import sleep
from rich import print #pip install rich
from nltk.corpus import stopwords # pip install nltk (natural language took kit for NLP natural laguage processing)
# stopwords -> the, is, in, at...
import codecs # NLTK codecs manage text encoding/decoding to ensure proper handling of multilingual files and prevent encoding errors in NLP tasks.
import re # NLTK (regular expressions) are used to match, search, and manipulate text patterns
import string # text data that can be processed, tokenized, or analyzed, serving as input for various NLP operations like tokenization, parsing, or classification.


def preprocess_text(tweet):
        print ("*** raw tweet text")
        print (tweet)
        tweet = codecs.decode(tweet, 'unicode_escape')  # remove escape characters
        print ("*** after removing unicode espace chars")
        print (tweet)
        #tweet = tweet[2:-1]
        #print ("*** after left & right deleting")
        print (tweet)
        tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet)
        print ("*** after replacing www.* by URL")
        print (tweet)
        tweet = re.sub('[^\x00-\x7f]', '', tweet)
        print ("*** remove non ASCII chars (final touch)")
        print (tweet)
        tweet = re.sub('@[^\s]+', 'USER', tweet)
        print ("*** after replacing @user by USER")
        print (tweet)
        tweet = re.sub('RT', '', tweet)
        print ("*** after delete RT")
        print (tweet)
        tweet = tweet.lower().replace("ё", "е")
        print ("*** after replacing ë by e")
        print (tweet)
        tweet = re.sub('[^a-zA-Zа-яА-Я1-9]+', ' ', tweet)
        print ("*** after replacing no latin or cyrilic chars by space char")
        print (tweet)
        tweet = re.sub(' +', ' ', tweet)
        print ("*** after replacing multiple space chars by one")
        print (tweet)
        return tweet.strip()

"""
# Define the path to the JSON file
json_file_path = 'data.json'

# Read the JSON file
with open(json_file_path, 'r') as file:
    for line in file:
        # Parse each line as a JSON object
        tweet_data = json.loads(line)

        # Access the individual fields
        id_str = tweet_data['id_str']
        username = tweet_data['username']
        tweet = tweet_data['tweet']
        location = tweet_data['location']
        created_at = tweet_data['created_at']
        retweet_count = tweet_data['retweet_count']
        favorite_count = tweet_data['favorite_count']
        followers_count = tweet_data['followers_count']
        lang = tweet_data['lang']
        coordinates = tweet_data['coordinates']

        # Process the tweet data as needed
        print(f"ID: {id_str}")
        print(f"Username: {username}")
        print(f"Tweet: {tweet}")
        print(f"Location: {location}")
        print(f"Created At: {created_at}")
        print(f"Retweet Count: {retweet_count}")
        print(f"Favorite Count: {favorite_count}")
        print(f"Followers Count: {followers_count}")
        print(f"Language: {lang}")
        print(f"Coordinates: {coordinates}")
        print("--------------------")
        
        tweet  = preprocess_text(tweet)
        print ("***** AFTER TWEET STRIPPING")
        print (tweet)
        print("--------------------")
        #sleep(1)
"""
tweet = "abcd efg hijk"
tweet  = preprocess_text(tweet)
print ("***** AFTER TWEET STRIPPING")
print (tweet)
print("--------------------")
