# Spritzer Tweets

Download and process tweets from the ["Spritzer" Twitter archive](https://archive.org/details/archiveteam-twitter-stream-2019-05)

## Requires

`python >= 3.6`

## What to do

0. Install requirements

```
git submodule init
git submodule update
pip install -r requirements.txt
```

1. Download data

```
python bin/get_data.py
```

Warning: this will take *hours*


2. Unzip data

```
cd data
find . -name "*.tar" | xargs -n1 tar xf
# -P4 is for 4 parallel processes
find . -name *.bz2 | xargs -n1 bzip2 _P4 -d
cd ..
```

3. Generate plain text file

```
python bin/generate_text_file tweets.txt
```

or...

Save to mongo database

```
python bin/save_to_mongo.py <mongo_db> <lang>
```

Note that `bin/save_to_mongo.py` also erases files as they are processed


## Scraping more data from users

After saving to mongo, one thing we can do to expand our database is to fetch tweets from the user we got in the previous stage.

To do so, just run

```
python get_tweets_from_users.py <mongo_db> <app_files>
```

Beware that this will take days too! So run it and go do something else
