import requests
import praw
import json
from confluent_kafka import Producer
from datetime import datetime
import pandas as pd


client_key = 'client_key'
secret_key = 'secret_key'


auth_data = {'auth_data'
}

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def send_to_kafka(dataframe):
    bootstrap_servers = 'localhost:9092'
    topic = 'spark-cluster'
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    json_records = dataframe.to_json(orient='records')

    for record in json.loads(json_records):
        producer.produce(topic, key=str(record['id']), value=json.dumps(record), callback=delivery_report)

    producer.flush()

def scrap_api(subreddit='batman', headers=None):
    # Define client key, secret key, and auth data

    # Define default headers if not provided
    if headers is None:
        headers = {'User-Agent': 'Batman/0.0.1'}

    res = requests.get(f'https://oauth.reddit.com/r/{subreddit}/hot', headers=headers)
    data = res.json()
    children = data['data']['children']

    raw_data = []
    for post in children:
        post_data = post['data']
        raw_data.append({
            'title': post_data['title'],
            'author': post_data['author'],
            'score': post_data['score'],
            'id': post_data['id'],
            'created': datetime.utcfromtimestamp(post_data['created']).strftime('%Y-%m-%d %H:%M:%S UTC'),
            'num_comments': post_data['num_comments']
        })

    raw_df = pd.DataFrame(raw_data)

    reddit = praw.Reddit(client_id=client_key,
                         client_secret=secret_key,
                         username=auth_data['username'],
                         password=auth_data['password'],
                         user_agent='Batman/0.0.1')

    def sub_com(id):
        submission = reddit.submission(id=id)
        top_level_comment_bodies = []  
        sub_comment_bodies = []  

        def process_comments(comments):
            for comment in comments:
                if isinstance(comment, praw.models.Comment):
                    sub_comment_bodies.append(comment.body)  
                    process_comments(comment.replies)  

        for top_level_comment in submission.comments:
            if isinstance(top_level_comment, praw.models.Comment):
                top_level_comment_bodies.append(top_level_comment.body)  
                process_comments(top_level_comment.replies)  

        return top_level_comment_bodies, sub_comment_bodies

    def comments_to_json(sub_com_id):
        top_level_comments, sub_comments = sub_com(sub_com_id)

        min_length = min(len(top_level_comments), len(sub_comments))
        top_level_comments = top_level_comments[:min_length]
        sub_comments = sub_comments[:min_length]

        data = {
            'sub_com_id': [sub_com_id] * min_length,
            'main_comment': top_level_comments,
            'reply': sub_comments
        }
        comments_df = pd.DataFrame(data)

        json_data = comments_df.to_json(orient='records')

        return json_data
    
    raw_df['comments'] = raw_df['id'].apply(comments_to_json)

    return raw_df

# Uncomment and define the send_to_kafka function if needed
# def send_to_kafka(dataframe):
#     ...

if __name__ == "__main__":
    # Set your client key, secret key, and auth data

    auth = requests.auth.HTTPBasicAuth(client_key, secret_key)
    headers = {'User-Agent': 'Batman/0.0.1'}
    res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=auth_data, headers=headers)

    TOKEN = res.json()['access_token']
    headers['Authorization'] = f'bearer {TOKEN}'
    
    raw_df = scrap_api(subreddit='batman',headers=headers)
    
    send_to_kafka(raw_df)