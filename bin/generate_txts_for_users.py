import os
import fire
import pymongo
from tqdm import tqdm


def generate_txts_for_users(database, out_dir):
    """
    Generate plain text file from Tweets

    """
    print(f"Connecting to {database}")
    client = pymongo.MongoClient()
    db = client[database]

    query = {
        "processed": True
    }

    for user in tqdm(db.users.find(query), total=db.users.count_documents(query)):
        file_path = os.path.join(out_dir, f"{user['id']}.txt")

        if os.path.exists(file_path):
            continue
        if db.tweets.count_documents({"user_id": user["id"]}) >= 50:


            tweets = db.tweets.find({"user_id": user["id"]}, {"text": 1})

            text = "\n".join(t["text"] for t in tweets)

            with open(file_path, "w") as f:
                f.write(text)



if __name__ == "__main__":
    fire.Fire(generate_txts_for_users)
