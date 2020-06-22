import os
import fire
import json
import glob
import queue
import datetime
import time
import threading
import pymongo
import tweepy
from tweepyrate import create_apps, get_user_tweets
from tqdm.auto import tqdm

def save_tweets_from_user(user_id, db, app):
    try:
        tweets = get_user_tweets(
            app, user_id, count=1000, timeout=200,
            wait_on_rate_limit=True, wait_on_rate_limit_notify=True
        )
    except tweepy.TweepError as e:
        raise e
    for tweet in tweets:
        tweet = tweet._json
        try:
            if 'retweeted_status' in tweet:
                tweet = tweet['retweeted_status']

            db.tweets.insert_one({
                "text": tweet["full_text"],
                "tweet_id": tweet["id"],
                "user_id": tweet["user"]["id"],
                "screen_name": tweet["user"]["screen_name"],
            })
        except pymongo.errors.DuplicateKeyError as e:
            continue


def get_tweets_from_users(database, app_file):
    """
    Save tweets to mongo

    Arguments:

    base_directory: string
        Path to directory where jsons are located

    output_path: string
        Where to write all the plain text

    num_files: int (optional)

    """

    apps = create_apps(app_file)
    for app in apps:
        print(app.me().screen_name)
    print(f"Connecting to {database}")
    client = pymongo.MongoClient()
    db = client[database]

    print("Ensuring indices")

    db.tweets.create_index("tweet_id", unique=True)
    db.users.create_index("user_id", unique=True)

    pbar = tqdm(total=db.users.count({"processed": {"$ne": True}}))
    q = queue.Queue()
    stopping = threading.Event()

    def create_worker(app):
        def worker(timeout=1):
            client = pymongo.MongoClient()
            db = client[database]
            while not stopping.is_set():
                try:
                    user_id = q.get(True, timeout)
                    save_tweets_from_user(user_id, db, app)
                    db.users.update_one(
                        {"user_id": user_id},
                        {"$set": {"processed": True}},
                    )
                    pbar.update()
                    q.task_done()
                except tweepy.TweepError as e:
                    print(e)
                    q.task_done()
                    pbar.update()
                except queue.Empty:
                    pass
        return worker

    threads = []
    for app in apps:
        t = threading.Thread(target=create_worker(app))
        t.start()
        threads.append(t)

    for user in db.users.find({"processed": {"$ne": True}}, {"user_id": 1}):
        q.put(user["user_id"])

    q.join()
    stopping.set()


if __name__ == "__main__":
    fire.Fire(get_tweets_from_users)
