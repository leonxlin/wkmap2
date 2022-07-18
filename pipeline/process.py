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

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.metrics import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from smart_open import open as smart_open

import pipeline.dump_readers as dump_readers
import pipeline.categorization as categorization



def create_inputs(p, args):
	if args.categorylinks_dump:
		categorylinks = (p
			| 'ReadCategorylinks'
			>> dump_readers.CategorylinksDumpReader(
				args.categorylinks_dump, max_lines=args.max_readlines))
	else:
		categorylinks = (p
			| 'CreateCategorylinks'
			>> beam.Create([
				dump_readers.Categorylink(page_id=1, category='Animals'),
				dump_readers.Categorylink(page_id=2, category='Planets'),
				dump_readers.Categorylink(page_id=3, category='Planets'),
				dump_readers.Categorylink(page_id=4, category='Animals'),
				]))

	if args.page_dump:
		pages = (p
			| 'ReadPages'
			>> dump_readers.PageDumpReader(
				args.page_dump, max_lines=args.max_readlines))
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
				]))

	if args.wikidata_dump:
		entities = (p
			| 'ReadEntities'
			>> dump_readers.WikidataJsonDumpReader(
				args.wikidata_dump, max_lines=args.max_readlines))
	else:
		entities = (p
			| 'read entities'
			>> beam.Create([
				dump_readers.Entity(qid='Q5', title='Category:Animals'),
				dump_readers.Entity(qid='Q6', title='Category:Planets'),
				dump_readers.Entity(qid='Q1', title='Beaver'),
				dump_readers.Entity(qid='Q2', title='Mercury'),
				dump_readers.Entity(qid='Q3', title='Uranus'),
				dump_readers.Entity(qid='Q4', title='Ant'),
				]))

	return categorylinks, pages, entities


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
	  '--categorylinks_dump',
	  type=str,
	  dest='categorylinks_dump',
	  help='Path to copy of enwiki-????????-categorylinks.sql[.gz] (local or GCS).')
	parser.add_argument(
	  '--page_dump',
	  type=str,
	  dest='page_dump',
	  help='Path to copy of enwiki-????????-page.sql[.gz] (local or GCS).')
	parser.add_argument(
	  '--wikidata_dump',
	  type=str,
	  dest='wikidata_dump',
	  help='Path to copy of wikidata-????????-all.json[.gz] (local or GCS).')
	parser.add_argument(
	  '--max_readlines',
	  type=int,
	  dest='max_readlines',
	  default=None,
	  help='Maximum number of lines to read from each dumpfile.')
	parser.add_argument(
	  '--output',
	  type=str,
	  dest='output',
	  default='/tmp/process_out.txt',
	  help='Output file.')
	parser.add_argument(
	  '--output_metrics',
	  type=str,
	  dest='output_metrics',
	  default='/tmp/process_metrics.txt',
	  help='File to output beam metrics to.')
	args, pipeline_args = parser.parse_known_args(argv)

	pipeline_options = PipelineOptions(pipeline_args)
	pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

	p = None
	with beam.Pipeline(options=pipeline_options) as p:
		categorylinks, pages, entities = create_inputs(p, args)

		output = categorization.ConvertCategorylinksToQids(categorylinks, pages, entities)
		output | 'WriteOutput' >> WriteToText(args.output)

	with smart_open(args.output_metrics, 'w') as metrics_file:
		metrics_file.write(get_metrics_str(p))

if __name__ == '__main__':
	logging.getLogger().setLevel(logging.INFO)
	run()
