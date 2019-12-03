import os
import subprocess

base="https://archive.org/download/archiveteam-twitter-stream-2019-05/twitter_stream_2019_05_{:02}.tar"

if __name__ == '__main__':
    for i in range(1, 32):
        url = base.format(i)

        filename = os.path.basename(url)
        path = os.path.join("data/", filename)
        if os.path.exists(path):
            print(f"{filename} ya bajado")
        else:
            print(f"{filename} no existe, bajando")
            os.system(f"wget {url} -P data/")
