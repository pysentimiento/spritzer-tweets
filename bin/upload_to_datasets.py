"""
Upload tweets to datasets
"""
import os
import fire
import glob
from datasets import load_dataset
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def upload_to_datasets(input_path, dataset_name, cache_dir=None, limit=None):
    """
    Upload tweets to datasets
    """

    # List gz files in input_path
    files = glob.glob(os.path.join(input_path, "*.gz"))

    if limit:
        files = files[:limit]

    train_files = files[:-1]
    test_files = files[-1:]

    logger.info(f"Train files: {train_files}")
    logger.info(f"Test files: {test_files}")

    # Upload train files

    ds = load_dataset("json", data_files={
        "train": train_files, "test": test_files},
        cache_dir=cache_dir
    )

    logger.info("Uploading dataset")

    ds.push_to_hub(dataset_name)


if __name__ == "__main__":
    fire.Fire(upload_to_datasets)
