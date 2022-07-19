"""Split a large file by lines using Beam.

Each line may end up in an arbitrary output file.

Example invocation:

"""

import argparse
import logging

import apache_beam as beam
from apache_beam.io.textio import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):
    arg_parser = argparse.ArgumentParser(description='Split a large file by lines.')
    arg_parser.add_argument('--input', type=str, 
            required=True, help='File to split. Local or gs:// path.')
    arg_parser.add_argument('--output_prefix', type=str, 
            required=True, help='Prefix for output shard files. Local or gs:// path.')
    arg_parser.add_argument('--output_suffix', type=str, 
            default='.gz', help='Suffix for output shard files. Determines compression.')
    arg_parser.add_argument('--num_shards', type=int, default=24,
            help='Number of shards to split file into.')
    args, pipeline_args = arg_parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        (p 
            | ReadFromText(args.input, coder=beam.coders.coders.BytesCoder())
            | WriteToText(
                args.output_prefix, 
                file_name_suffix=args.output_suffix, 
                coder=beam.coders.coders.BytesCoder(),
                num_shards=args.num_shards))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
