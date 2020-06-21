import os
import fire
import json
import glob
import queue
import threading
import pymongo
from tqdm.auto import tqdm

def save_tweets(json_file, lang, db, followers_count_threshold=10_000):
    tweets = []
    with open(json_file) as f:
        for line in f:
            try:
                tweet = json.loads(line)
                if 'retweeted_status' in tweet:
                    tweet = tweet['retweeted_status']

                if 'lang' in tweet and tweet['lang'] == lang:
                    try:
                        text = tweet["extended_tweet"]["full_text"]
                    except KeyError:
                        text = tweet['text']

                    user = tweet["user"]

                    if user["followers_count"] > followers_count_threshold:
                        continue
                    db.tweets.insert_one({
                        "text": text,
                        "tweet_id": tweet["id"],
                        "user_id": tweet["user"]["id"],
                        "screen_name": tweet["user"]["screen_name"],
                    })

                    user["user_id"] = user["id"]
                    db.users.insert_one(user)
            except pymongo.errors.DuplicateKeyError as e:
                continue
            except json.JSONDecodeError:
                continue


def save_to_mongo(database, lang, num_workers=4):
    """
    Save tweets to mongo

    Arguments:

    base_directory: string
        Path to directory where jsons are located

    output_path: string
        Where to write all the plain text

    num_files: int (optional)

    """

    glob_path = "data/**/**/*.json"
    print(f"Looking for {glob_path}")
    jsons = glob.glob(glob_path)

    print(f"{len(jsons)} found")
    print(f"Connecting to {database}")
    client = pymongo.MongoClient()
    db = client[database]

    print("Ensuring indices")

    db.tweets.create_index("tweet_id", unique=True)
    db.users.create_index("user_id", unique=True)

    print(f"Creating {num_workers} threads")
    pbar = tqdm(total=len(jsons))
    q = queue.Queue()
    stopping = threading.Event()

    def worker(timeout=1):
        client = pymongo.MongoClient()
        db = client[database]
        while not stopping.is_set():
            try:
                json_file = q.get(True, timeout)
                save_tweets(json_file, lang, db)
                pbar.update()
                os.remove(json_file)
                q.task_done()
            except queue.Empty:
                pass


    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for json_file in jsons:
        q.put(json_file)

    q.join()
    stopping.set()


if __name__ == "__main__":
    fire.Fire(save_to_mongo)
