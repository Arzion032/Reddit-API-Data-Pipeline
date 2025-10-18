from utils.contants import *
from etls.reddit_extract import reddit_conn, extract_posts

def extract_reddit(file_name: str, subreddit: str, time_filter='day', limit=None):
    # Connect and extract from reddit
    reddit=reddit_conn(CLIENT_ID, SECRET, 'Airscholar Agent')
   
   
    post = extract_posts(reddit, subreddit, time_filter, limit)