import os
import fire
import motor.motor_asyncio
import asyncio
import aiofiles
import re
from tqdm.asyncio import tqdm
from pysentimiento.preprocessing import preprocess_tweet

preprocess_args = {
    "user_token": "@usuario",
    "url_token": "URL",
    "hashtag_token": "hashtag",
    "emoji_wrapper": "emoji",
}

def my_preprocess(tweet):

    ret = preprocess_tweet(tweet, **preprocess_args)
    ret = re.sub("\n+", ". ", ret)
    ret = re.sub(r"\s+", " ", ret)
    return ret.strip()



async def worker(name, queue, pbar, out_dir, minimal_preprocess):
    file_path = os.path.join(out_dir, f"{name}.txt")

    async with aiofiles.open(file_path, "w+") as f:
        tweets = []
        while True:
            # Get a "work item" out of the queue.
            tweet = await queue.get()

            # Sleep for the "sleep_for" seconds.
            tweets.append(my_preprocess(tweet["text"]))

            if len(tweets) > 4_000:
                await f.write("\n".join(tweets) + "\n")
                tweets = []

            # Notify the queue that the "work item" has been processed.
            pbar.update()
            queue.task_done()


async def main(database, out_dir, preprocess, num_workers=32):
    """
    Event loop
    """
    print(f"Connecting to {database}")
    client = motor.motor_asyncio.AsyncIOMotorClient()
    db = client[database]

    print("Contando...")

    tweets = db.tweets.find()
    pbar = tqdm(total=await db.tweets.estimated_document_count())

    print("Comenzando!")

    queue = asyncio.Queue()


    # Create three worker tasks to process the queue concurrently.
    tasks = []
    for i in range(num_workers):
        task = asyncio.create_task(worker(f'spanish-tweets-{str(i).zfill(3)}', queue, pbar, out_dir))
        tasks.append(task)


    # Generate random timings and put them into the queue.
    total_sleep_time = 0
    async for tweet in tweets:
        queue.put_nowait(tweet)

    await queue.join()
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)

def generate_txts_for_users(database, out_dir, preprocess=False, num_workers=8):
    """
    Generate plain text file from Tweets

    """

    asyncio.run(main(database, out_dir, preprocess, num_workers))



if __name__ == "__main__":
    fire.Fire(generate_txts_for_users)
