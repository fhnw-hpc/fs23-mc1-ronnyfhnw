import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from emot.emo_unicode import UNICODE_EMOJI, EMOTICONS_EMO
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import unicodedata
from datetime import datetime
import h5py
import numpy as np
import os
import time
from kafka_functions import *

check_kafka(twitter_topic)

twitter_consumer = init_twitter_consumer()

class TextTransformer:
    '''
    Class for cleaning up tweet text.
    '''
    def __init__(self):
        self.tokenizer = RegexpTokenizer(r'\w+')
        self.stopwords = set(stopwords.words("english"))
        self.stopwords.update(('and', 'I', 'A', 'http', 'And', 'So', 'arnt', 'This', 'When', 'It', 'many', 'Many', 'so', 'cant', 'Yes', 'yes', 'No', 'no', 'These', 'these', 'mailto', 'regards', 'ayanna', 'like', 'email'))
        
    def transform(self, text):
        text = self.emoji(text)
        text = self.remove_links(text)
        text = self.remove_users(text)
        text = self.email_address(text)
        text = self.punct(text)
        text = self.lower(text)
        text = self.removeStopWords(text)
        text = self.remove_(text)
        text = self.non_ascii(text)
        return text
        
    def emoji(self, text):
        for emot in UNICODE_EMOJI:
            if text == None:
                text = text
            else:
                text = text.replace(emot, "_".join(UNICODE_EMOJI[emot].replace(",", "").replace(":", "").split()))
        return text

    def remove_links(self, text):
        text = re.sub(r'http\S+', '', text)  # remove http links
        text = re.sub(r'bit.ly/\S+', '', text)  # remove bitly links
        text = text.strip('[link]')  # remove [links]
        return text

    def remove_users(self, text):
        text = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', text)  # remove tweeted at
        text = re.sub('(RT[\s]+)', '', text)  # remove retweet
        return text

    def email_address(self, text):
        email = re.compile(r'[\w\.-]+@[\w\.-]+')
        return email.sub(r'', text)

    def punct(self, text):
        text = self.tokenizer.tokenize(text)
        text = " ".join(text)
        return text

    def lower(self, text):
        return text.lower()

    def removeStopWords(self, text):
        new_text = ' '.join([word for word in text.split() if word not in self.stopwords])
        return new_text

    def remove_(self, text):
        text = re.sub('([_]+)', "", text)
        return text

    def non_ascii(self, text):
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
        return text

tweet_transformer = TextTransformer()

# initialize sentiment analyzer
nltk.download('vader_lexicon')
analyzer = SentimentIntensityAnalyzer()

start = datetime.now()
if __name__ == '__main__':
    while True:
        # determine and load file
        filename = "data/twitter/tweets_" + datetime.now().strftime("%d-%m-%Y") + ".h5"

        if (datetime.now() - start).seconds >= 10:
            # consume messages and process
            messages = consume_messages(twitter_consumer)
            start = datetime.now()
            

            # loop over tweets and analze sentiment
            tweet_data = {"created_at": [], "text": [], "negative": [], "neutral": [], "positive": []}

            for i, message in enumerate(messages):
                value = json.loads(message[1])
                tweet_data["created_at"].append(value['created_at'])
                text = tweet_transformer.transform(value['text'])
                tweet_data["text"].append(text)
                sentiment_scores = analyzer.polarity_scores(text) # 'neg': 0.0, 'neu': 1.0, 'pos': 0.0,
                tweet_data["negative"].append(sentiment_scores['neg'])
                tweet_data["neutral"].append(sentiment_scores['neu'])
                tweet_data["positive"].append(sentiment_scores['pos'])

            print(tweet_data)
                
            # append to file
            if filename[13:] in os.listdir("data/twitter/"):
                with h5py.File(filename, 'a') as hf:
                    tweet_group = hf['tweets']
                    num_tweets = len(tweet_group['created_at']) # get the number of existing tweets
                    
                    # append new tweet data to each dataset
                    for key in tweet_data.keys():
                        new_data = tweet_data[key]
                        tweet_group[key].resize((num_tweets + len(new_data),))
                        tweet_group[key][num_tweets:num_tweets+len(new_data)] = new_data
                print("appended")
            # create new file
            else:
                with h5py.File(filename, 'w') as hf:
                    # create a group to store the tweet data
                    tweet_group = hf.create_group('tweets')

                    # create new datasets for each key in the tweet data
                    for key in tweet_data.keys():
                        if key == 'created_at' or key == 'text':
                            tweet_group.create_dataset(key, (0,), maxshape=(None,), dtype=h5py.string_dtype(encoding='utf-8'))
                        else:
                            tweet_group.create_dataset(key, (0,), maxshape=(None,))
                        tweet_group[key].resize((len(tweet_data[key]),))
                        tweet_group[key][:] = tweet_data[key]
                print("created")

        elif 10 - (datetime.now() - start).seconds > 1:
            sleep_time = 10 - (datetime.now() - start).seconds
            print(f"Sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)