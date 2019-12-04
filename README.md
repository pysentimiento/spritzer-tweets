# English Tweets

Download and process tweets from the ["Spritzer" Twitter archive](https://archive.org/details/archiveteam-twitter-stream-2019-05)

## Requires

`python >= 3.6`

## What to do

1. Download data

```
python bin/get_data.py
```

Warning: this will take *hours*


2. Unzip data

```
cd data
find . -name "*.tar" | xargs -n1 tar xf
find . -name *.bz2 | xargs -n1 bzip2 -d
cd ..
```
