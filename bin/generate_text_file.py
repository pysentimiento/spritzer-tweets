import os
import fire
import json
import glob
from tqdm import tqdm

def generate_text_file(output_path, num_files=None):
    """
    Generate plain text file from Tweets

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



if __name__ == "__main__":
    fire.Fire(generate_text_file)
