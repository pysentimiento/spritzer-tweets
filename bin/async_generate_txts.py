import os
import fire
import motor.motor_asyncio
import asyncio
import aiofiles
from tqdm.asyncio import tqdm
from pysentimiento.preprocessing import preprocess_tweet

preprocess_args = {
    "user_token": "usuario",
    "url_token": "URL",
    "hashtag_token": "hashtag",
    "emoji_wrapper": "emoji",
}

def my_preprocess(*args):
    return preprocess_tweet(*args, **preprocess_args)

async def process_user(user, path):
    """
    Process a single user's tweets and save
    """
    if os.path.exists(path) or len(user["tweets"]) < 10:
        return

    tweets = [my_preprocess(t["text"].replace("\n", ". ").lower()) for t in user["tweets"]]

    async with aiofiles.open(path, "w+") as f:
        for tweet in tweets:
            await f.write(tweet + "\n")

async def main(database, out_dir, preprocess):
    """
    Event loop
    """
    print(f"Connecting to {database}")
    client = motor.motor_asyncio.AsyncIOMotorClient()
    db = client[database]

    query = {
        #"processed": True
    }
    print("Contando...")
    total_users = await db.users.count_documents(query)
    pbar = tqdm(total=total_users)

    users_and_tweets = db.users.aggregate([
        {"$lookup": {"from": "tweets", "localField": "id", "foreignField": "user_id", "as":"tweets"}},
        {"$project": {"id": 1, "screen_name": 1, "tweets.text": 1}},
    ])
    async for user in users_and_tweets:
        file_path = os.path.join(out_dir, f"{user['screen_name'].lower()}-{user['id']}.txt")
        await process_user(user, file_path)
        pbar.update()

def generate_txts_for_users(database, out_dir, preprocess=False):
    """
    Generate plain text file from Tweets

    """

    asyncio.run(main(database, out_dir, preprocess))



if __name__ == "__main__":
    fire.Fire(generate_txts_for_users)
