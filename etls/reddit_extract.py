import praw, sys
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
        
def extract_posts(reddit: Reddit, subreddit: str, time_filter: str, limit=None):
    subreddit = reddit.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)
    
    post_lists = []
    
    for post in posts:
        post_dict = vars(post)
        print(post_dict)
        # post = {key: post_dict[key] for key in POST_FIELDS}