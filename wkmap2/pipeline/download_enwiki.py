"""Download wikidump files and optionally upload them to google cloud storage."""

import argparse
from google.cloud import storage
from multiprocessing import Pool
import urllib.request
import os

arg_parser = argparse.ArgumentParser(description='Download wikidump files from URL.')
arg_parser.add_argument('--filenames', type=str, 
        default="pipeline/enwiki_filenames.txt", 
        help='File containing list of filenames to download, one per line.')
arg_parser.add_argument('--url_prefix', type=str, 
        default="http://dumps.wikimedia.your.org/enwiki/20220701/", 
        help='Prefix to prepend to filenames provided in --filenames.')
arg_parser.add_argument('--local_dir', type=str, 
        default="data", 
        help='Local directory to download files to. Will skip download of files '
             'that already exist in this directory.')
arg_parser.add_argument('--bucket', type=str, 
        default="wikidumps", 
        help='Bucket to upload files to. Will skip upload if empty.')
arg_parser.add_argument('--workers', type=int, 
        default=4, 
        help='Number of parallel processes.')
args = arg_parser.parse_args()


def download(filename: str):
    url = os.path.join(args.url_prefix, filename)
    dest = os.path.join(args.local_dir, filename)
    if os.path.exists(dest):
        print(f"Found {dest}; skipping download.")
        return

    print(f"Downloading {url} ...")
    urllib.request.urlretrieve(url, dest)
    print(f"Finished downloading {url}.")


def upload(filename: str):
    source = os.path.join(args.local_dir, filename)
    if not os.path.exists(source):
        print(f"Something went wrong; couldn't find {source}.")
        return

    print(f"Uploading {filename} ...")
    client = storage.Client()
    bucket = client.bucket(args.bucket)
    blob = bucket.blob(filename)

    blob.upload_from_filename(source)
    print(f"Finished uploading {filename}.")


def process_file(filename: str):
    download(filename)

    if args.bucket:
        upload(filename)
    

def run():

    filenames = open(args.filenames, 'r').readlines()
    filenames = [s.strip() for s in filenames]
    filenames = [s for s in filenames if s]
    print(f"Found {len(filenames)} filenames.")

    os.makedirs(args.local_dir, exist_ok=True)

    with Pool(args.workers) as p:
        p.map(process_file, filenames)


if __name__ == "__main__":
    run()

