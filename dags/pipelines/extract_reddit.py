from utils.contants import *
from etls.reddit_extract import extract_posts, reddit_conn, transform_data, load_data_to_csv
import pandas as pd

def extract_reddit(file_name: str, subreddit: str, time_filter='day', limit=None):
    # Connect and extract from reddit
    reddit=reddit_conn(CLIENT_ID, SECRET, 'Melvin Sarabia')
    
    # Extract subreddit post
    post = extract_posts(reddit, subreddit, time_filter, limit)
   
    # Transform the data
    post_df = pd.DataFrame(post)
    post_df = transform_data(post_df)
    
    # Load data as csv
    file_path = f"{OUTPUT_PATH}/{file_name}.csv"
    load_data_to_csv(post_df, file_path)
    
    return file_path