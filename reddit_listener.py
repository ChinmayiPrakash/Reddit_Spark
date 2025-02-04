# reddit_listener.py
import praw
from kafka import KafkaProducer
from afinn import Afinn
import json
import time
from reddit_config import REDDIT_CLIENT_ID, REDDIT_SECRET, REDDIT_USER_AGENT

# Set up Reddit API
reddit = praw.Reddit(client_id=REDDIT_CLIENT_ID, 
                     client_secret=REDDIT_SECRET,
                     user_agent=REDDIT_USER_AGENT)

# Set up Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Set up Afinn sentiment analysis
afinn = Afinn()

# Create a listener for the chosen subreddit
def reddit_listener(subreddit_name):
    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.stream.submissions():
        # Get the submission text and perform sentiment analysis
        text = submission.title + " " + submission.selftext
        sentiment = afinn.score(text)
        
        # Prepare the message to send to Kafka
        message = {
            'text': text,
            'senti_val': sentiment
        }
        
        # Send the message to the Kafka topic
        producer.send('reddit', value=message)
        print(f"Sentiment: {sentiment}, Text: {text[:100]}...")  # Display first 100 characters for clarity
        
        time.sleep(1)

if __name__ == "__main__":
    subreddit_name = "GameOfThrones"  # Example subreddit
    reddit_listener(subreddit_name)
