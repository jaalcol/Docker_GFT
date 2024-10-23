import json
from json import dumps #convert dictionary to json
from time import sleep
from rich import print 
from kafka import KafkaProducer # pip install kafka-python

producer = KafkaProducer(bootstrap_servers=['docker-kafka-1:29092'],
                         value_serializer=lambda K:dumps(K).encode('utf-8'))

# Define the path to the JSON file
json_file_path = 'data.json'

# Read the JSON file
with open(json_file_path, 'r') as file:
    for line in file:
        # Parse each line as a JSON object
        tweet = json.loads(line)

        tweet_dir={
            "id_str": tweet['id_str'],
            "username": tweet['username'],
            "tweet": tweet['tweet'],
            "location": tweet['location'],
            "created_at": tweet['created_at'],
            "retweet_count": tweet['retweet_count'],
            "favorite_count": tweet['favorite_count'],
            "followers_count": tweet['followers_count'],
            "lang": tweet['lang'],
            "coordinates": tweet['coordinates']
        }

        producer.send('tweets-sim', value=tweet_dir)
        print(tweet_dir)
       
        sleep(1)
