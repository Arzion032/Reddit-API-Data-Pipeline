
def extract_reddit(file_name: str, subreddit: str, time_filter='day', limit=None):
    # Connect and extract from reddit
    
    reddit=connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')