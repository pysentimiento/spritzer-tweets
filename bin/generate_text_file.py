import os
import fire
import json
import glob
import pymongo
from tqdm import tqdm

def generate_from_path(base_directory, output_path, num_files):
    glob_path = os.path.join(base_directory, "**/**/*.json")
    print(f"Looking for {glob_path}")
    jsons = glob.glob(glob_path)
    print(f"{len(jsons)} found")

    tweets = []

    if num_files:
        jsons = jsons[:num_files]

    pbar = tqdm(jsons)
    total_count = 0
    for tweet_file in pbar:
        with open(tweet_file) as f:
            for line in f:
                try:
                    total_count += 1
                    tweet = json.loads(line)
                    if 'retweeted_status' in tweet:
                        tweet = tweet['retweeted_status']

                    if 'lang' in tweet and tweet['lang'] == "en":
                        try:
                            text = tweet["extended_tweet"]["full_text"]
                        except KeyError:
                            text = tweet['text']
                        tweets.append(text)
                except json.JSONDecodeError:
                    continue
            #pbar.set_description(f"{len(tweets) / 1000:.2f}K / {total_count / 1000:.2f}K english tweets so far")
    print(f"{len(tweets)} english tweets found")
    with open(output_path, "w+") as f:
        for text in tweets:
            f.write(text+"\n")
    print(f"Tweets written to {output_path}")


def generate_from_mongo(database, output_path):
    client = pymongo.MongoClient()
    db = client[database]

    tweets = db.tweets.find({}, {"text": 1})

    pbar = tqdm(tweets, total=db.tweets.count())

    with open(output_path, "w+") as f:
        for tweet in pbar:
            f.write(tweet["text"]+"\n")

    print(f"Tweets written to {output_path}")

def generate_text_file(source, output_path, database=None, base_directory="data", num_files=None):
    """
    Generate plain text file from Tweets

    Arguments:
    source: string
        "mongo" or "json" allowed

    database: string
        If source=="mongo", name of the database from which to get tweets

    base_directory: string
        If source=="json", Path to directory where jsons are located

    output_path: string
        Where to write all the plain text

    num_files: int (optional)

    """

    if source not in {"mongo", "json"}:
        print(f"source must be one of ['mongo', 'json']")
        return

    if source == "json":
        return generate_from_path(base_directory, output_path, num_files)
    else:
        return generate_from_mongo(database, output_path)



if __name__ == "__main__":
    fire.Fire(generate_text_file)
