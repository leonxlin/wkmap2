"""Split a large file by lines.

Each line may end up in an arbitrary output file.

Example invocation:

python3 pipeline/shard_lines.py \
--input=~/wkmap/data/wikipedia2vec_enwiki_20180420_300d.txt.bz2 \
--output=/tmp/shards-{}.txt.bz2 \
--max_lines_to_read=100000
"""

import argparse
from google.cloud import storage
from smart_open import open as smart_open
import os
import queue
import threading
import time


def populate_shardnames(names_queue=None, done_queue=None, name_format=None):
    if any(x is None for x in [names_queue, done_queue, name_format]):
        return

    i_shard = 0

    while True:
        if not done_queue.empty():
            return
        if not names_queue.empty():
            time.sleep(0.5)
            continue

        names_queue.put(name_format.format(str(i_shard).zfill(5)))
        i_shard += 1


def process_lines(lines_queue, names_queue, chars_per_shard=5e8):
    if lines_queue is None or names_queue is None:
        return

    done = False
    while not done:
        chars_written = 0
        filename = names_queue.get()

        with smart_open(filename, 'w') as file:
            print(f'Writing to {filename}...')
            while chars_written < chars_per_shard:
                line = lines_queue.get()
                if line is None:
                    done = True
                    break
                chars_written += file.write(line)

        print(f'Done writing to {filename}.')



def run():
    arg_parser = argparse.ArgumentParser(description='Split a large file by lines.')
    arg_parser.add_argument('--input', type=str, 
            default=None, required=True, 
            help='File to split. Local or gs:// path.')
    arg_parser.add_argument('--output', type=str, 
            default=None, required=True, 
            help='Format string for output files, containing {} for the shard number,'
                 ' e.g., "/tmp/hello-{}.txt" Local or gs:// path.')
    arg_parser.add_argument('--chars_per_shard', type=int, default=5e8,
            help='Target number of uncompressed characters to write per shard.')
    arg_parser.add_argument('--workers', type=int, default=8, 
            help='Number of threads.')
    arg_parser.add_argument('--max_lines_to_read', type=int, 
            default=None, 
            help='Maximum number of lines to read from --input.')
    args = arg_parser.parse_args()

    if '{}' not in args.output:
        args.output += '-{}'

    shardnames = queue.Queue() 
    lines = queue.Queue()
    done = queue.Queue()

    names_thread = threading.Thread(target=populate_shardnames, kwargs={
        'names_queue': shardnames, 'done_queue': done, 'name_format': args.output})
    names_thread.start()

    # TODO: Figure out whether parallelism actually helps here. Note: Using processes 
    # instead of threads ran into memory issues.
    workers = [
        threading.Thread(target=process_lines, args=(lines, shardnames, args.chars_per_shard))
        for _ in range(args.workers)
    ]
    for worker in workers:
        worker.start()

    with smart_open(args.input, 'r') as file:
        print(f'Reading from {args.input}...')
        for i, line in enumerate(file):
            if args.max_lines_to_read and (i >= args.max_lines_to_read):
                break
            lines.put(line)

    print(f'Finished reading from {args.input}.')

    # Use None entry in `lines` queue to signal workers to stop. Use
    # `done` queue to signal names_thread to stop. (There's probably a better
    # way...)
    for _ in range(args.workers):
        lines.put(None)
    for worker in workers:
        worker.join()
    done.put(True)
    names_thread.join()

    print('Done.')



if __name__ == "__main__":
    run()
