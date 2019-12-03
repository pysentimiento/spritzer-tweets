import os
import glob
from tqdm import tqdm

if __name__ == '__main__':
    zipfiles = glob.glob("data/**/**/*.bz2")

    for zipfile in tqdm(zipfiles):
        os.system(f"bzip2 -d {zipfile}")
