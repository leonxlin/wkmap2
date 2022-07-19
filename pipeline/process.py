"""Example invocation:

python3 -m pipeline.process \
--categorylinks_dump=pipeline/testdata/enwiki-20220701-categorylinks-50lines.sql \
--wikidata_dump=pipeline/testdata/wikidata-20220704-all-50lines.json \
--page_dump=pipeline/testdata/enwiki-20220701-page-55lines.sql

(Output will be empty as the test data is insufficient.)
"""

import argparse
import json
import logging
import os

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.metrics import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from smart_open import open as smart_open

import pipeline.dump_readers as dump_readers
import pipeline.categorization as categorization



def create_inputs(p, args):
    kwargs = {
        'max_lines': args.max_readlines
    }
    if not args.verify_headers:
        kwargs['expected_header'] = None

    if args.categorylinks_dump:
        categorylinks = (p
            | 'ReadCategorylinks'
            >> dump_readers.CategorylinksDumpReader(
                args.categorylinks_dump, **kwargs))
    else:
        categorylinks = (p
            | 'CreateCategorylinks'
            >> beam.Create([
                dump_readers.Categorylink(page_id=1, category='Animals'),
                dump_readers.Categorylink(page_id=2, category='Planets'),
                dump_readers.Categorylink(page_id=3, category='Planets'),
                dump_readers.Categorylink(page_id=4, category='Animals'),
                dump_readers.Categorylink(page_id=5, category='Things'),
                dump_readers.Categorylink(page_id=6, category='Things'),
            ]))

    if args.page_dump:
        pages = (p
            | 'ReadPages'
            >> dump_readers.PageDumpReader(
                args.page_dump, **kwargs))
    else:
        pages = (p
            | 'CreatePages'
            >> beam.Create([
                dump_readers.Page(page_id=1, title='Beaver'),
                dump_readers.Page(page_id=2, title='Mercury'),
                dump_readers.Page(page_id=3, title='Uranus'),
                dump_readers.Page(page_id=4, title='Ant'),
                dump_readers.Page(page_id=5, title='Animals', namespace=14),
                dump_readers.Page(page_id=6, title='Planets', namespace=14),
                dump_readers.Page(page_id=7, title='Things', namespace=14),
            ]))

    if args.wikidata_dump:
        entities = (p
            | 'ReadEntities'
            >> dump_readers.WikidataJsonDumpReader(
                args.wikidata_dump, require_title=True, **kwargs))
    else:
        entities = (p
            | 'read entities'
            >> beam.Create([
                dump_readers.Entity(qid='Q5', title='Category:Animals'),
                dump_readers.Entity(qid='Q6', title='Category:Planets'),
                dump_readers.Entity(qid='Q7', title='Category:Things'),
                dump_readers.Entity(qid='Q1', title='Beaver'),
                dump_readers.Entity(qid='Q2', title='Mercury'),
                dump_readers.Entity(qid='Q3', title='Uranus'),
                dump_readers.Entity(qid='Q4', title='Ant'),
            ]))

    if args.qrank_dump:
        qranks = (p
            | 'ReadQRanks'
            >> dump_readers.QRankDumpReader(
                args.qrank_dump, **kwargs))
    else:
        qranks = (p
            | 'read qranks'
            >> beam.Create([
                dump_readers.QRankEntry(qid='Q1', qrank=1),
                dump_readers.QRankEntry(qid='Q2', qrank=2),
                dump_readers.QRankEntry(qid='Q3', qrank=3),
                dump_readers.QRankEntry(qid='Q4', qrank=4),
            ]))

    return categorylinks, pages, entities, qranks


def get_metrics_str(pipeline):
    ret = {}

    results = pipeline.result.metrics().query(MetricsFilter())
    for key in results:
        ret[key] = []
        for result in results[key]:
            ret[key].append(str(result))
    return json.dumps(ret, indent=4)



def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--categorylinks-dump',
      type=str,
      help='Path to copy of enwiki-????????-categorylinks.sql[.gz] (local or GCS).')
    parser.add_argument(
      '--page-dump',
      type=str,
      help='Path to copy of enwiki-????????-page.sql[.gz] (local or GCS).')
    parser.add_argument(
      '--wikidata-dump',
      type=str,
      help='Path to copy of wikidata-????????-all.json[.gz] (local or GCS).')
    parser.add_argument(
      '--qrank-dump',
      type=str,
      help='Path to copy of qrank.csv[.gz] (local or GCS).')
    parser.add_argument(
      '--max-readlines',
      type=int,
      dest='max_readlines',
      default=None,
      help='Maximum number of lines to read from each dumpfile.')
    parser.add_argument(
      '--no-verify-headers',
      dest='verify_headers',
      action='store_false',
      help='Skip verification of dump file "headers". Use this setting for '
        'sharded dump files.')
    parser.add_argument(
      '--output',
      type=str,
      default='/tmp/process_out/',
      help='An output directory.')
    args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    p = None
    with beam.Pipeline(options=pipeline_options) as p:
        categorylinks, pages, entities, qranks = create_inputs(p, args)

        done, pending, ready = categorization.CreateCategoryIndex(
            categorylinks, pages, entities, qranks)
        done | 'WriteOutputDone' >> WriteToText(os.path.join(args.output, 'done_nodes'))
        pending | 'WriteOutputPending' >> WriteToText(os.path.join(args.output, 'pending_nodes'))
        ready | 'WriteOutputReady' >> WriteToText(os.path.join(args.output, 'ready_nodes'))

    with smart_open(os.path.join(args.output, 'metrics'), 'w') as metrics_file:
        metrics_file.write(get_metrics_str(p))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
