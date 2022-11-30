"""
Fix json dump from MongoDB
"""
import asyncio
import fire
from gzip import GzipFile
from tqdm.auto import tqdm
import json
import gzip
import glob
import orjson
import os
from async_files import FileIO


class MyGzipFile(FileIO):
    OPEN = GzipFile


def process_tweet(tweet: dict) -> dict:
    return {
        k: str(v) for k, v in tweet.items() if k in ["tweet_id", "text", "user_id"]
    }


async def process_file(path, output_path):
    print(f"Converting {path} to {output_path}")

    with gzip.open(path, 'rt') as f_input:
        with gzip.open(output_path, "wt") as f_output:
            async for line in tqdm(f_input, total=5_000_000):
                line = line.strip()
                new_tweet = process_tweet(orjson.loads(line))
                encoded = str(json.dumps(new_tweet, ensure_ascii=False))
                f_output.write(encoded + "\n")


async def main(input_dir, output_dir):
    # List gz files in input_path
    files = glob.glob(os.path.join(input_dir, "*.gz"))

    print(files)

    tasks = []
    for path in files:
        basename = os.path.split(path)[1]
        #output_path = os.path.splitext(basename)[0]
        output_path = os.path.join(output_dir, basename)

        tasks.append(process_file(path, output_path))

    await asyncio.gather(*tasks)


def fix_jsons(input_path, output_path):
    """
    Fix json dump from MongoDB
    """
    return asyncio.run(main(input_path, output_path))


if __name__ == "__main__":
    fire.Fire(fix_jsons)
