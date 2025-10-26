import praw, sys, pandas as pd, numpy as np
from praw import Reddit

def reddit_conn(client_id, client_secret, user_agent)-> Reddit:
  
    try:
        reddit = praw.Reddit(client_id=client_id,
                        client_secret=client_secret,
                        user_agent=user_agent)
        
        print("connected to reddit!")
        return reddit
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
        
def extract_posts(instance: Reddit, subreddit: str, time_filter: str, limit=None):
    """_summary_

    Args:
        instance (Reddit): _description_
        subreddit (str): _description_
        time_filter (str): _description_
        limit (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    subreddit_instance = instance.subreddit(subreddit)
    posts = subreddit_instance.top(time_filter=time_filter, limit=limit)

    post_lists = []

    for post in posts:
        # Get the top 5 comments for each post
        post.comments.replace_more(limit=0)
        top_comments = [c.body for c in post.comments[:5]]
        
        # Extract only the needed attributes
        post_data = {
            'id': post.id,
            'title': post.title,
            'selftext': post.selftext,
            'score': post.score,
            'num_comments': post.num_comments,
            'author': str(post.author) if post.author else None,
            'subreddit': str(post.subreddit),
            'subreddit_subscribers': post.subreddit.subscribers,
            'created_utc': post.created_utc,
            'url': post.url,
            'over_18': post.over_18,
            'edited': post.edited,
            'spoiler': post.spoiler,
            'stickied': post.stickied,
            'upvote_ratio': post.upvote_ratio,
            'comments': top_comments
        }

        post_lists.append(post_data)

    return post_lists

def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True,False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['upvote_ratio'] = post_df['upvote_ratio'].astype(int)
    post_df['selftext'] = post_df['selftext'].astype(str)
    post_df['title'] = post_df['title'].astype(str)


    return post_df

def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)
    
    
    
    
    
    