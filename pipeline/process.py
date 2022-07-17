"""Example invocation:

python3 -m pipeline.process
"""

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.metrics import MetricsFilter

import pipeline.dump_readers as dump_readers
import pipeline.categorization as categorization


def run(p):
	categorylinks = (p 
		| 'read catlinks' 
		>> beam.Create(dump_readers.CategorylinksDumpReader(
			'pipeline/testdata/enwiki-20220701-categorylinks-50lines.sql')))
	pages = (p 
		| 'read pages' 
		>> beam.Create(dump_readers.PageDumpReader(
			'pipeline/testdata/enwiki-20220701-page-55lines.sql')))
	entities = (p 
		| 'read entities' 
		>> beam.Create(dump_readers.WikidataJsonDumpReader(
			'pipeline/testdata/wikidata-20220704-all-50lines.json')))

	cat_qid_to_member_qid = categorization.ConvertCategorylinksToQids(
		categorylinks, pages, entities)

	cat_qid_to_member_qid | 'write' >> WriteToText('/tmp/process.txt')



def run2(p):
	categorylinks = (p 
		| 'read catlinks' 
		>> beam.Create(dump_readers.CategorylinksDumpReader(
			'data/enwiki-20220701-categorylinks.sql', max_lines=100)))
	pages = (p 
		| 'read pages' 
		>> beam.Create(dump_readers.PageDumpReader(
			'data/enwiki-20220701-page.sql', max_lines=100)))
	entities = (p 
		| 'read entities' 
		>> beam.Create(dump_readers.WikidataJsonDumpReader(
			'data/wikidata-20220704-all.json.gz', max_lines=100000)))

	cat_qid_to_member_qid = categorization.ConvertCategorylinksToQids(
		categorylinks, pages, entities)

	cat_qid_to_member_qid | 'write' >> WriteToText('/tmp/process.txt')



def run3(p):
	categorylinks = (p 
		| 'read catlinks' 
		>> beam.Create([
			dump_readers.Categorylink(page_id=1, category='Animals'),
			dump_readers.Categorylink(page_id=2, category='Planets'),
			dump_readers.Categorylink(page_id=3, category='Planets'),
			dump_readers.Categorylink(page_id=4, category='Animals'),
			]))
	pages = (p 
		| 'read pages' 
		>> beam.Create([
			dump_readers.Page(page_id=1, title='Beaver'),
			dump_readers.Page(page_id=2, title='Mercury'),
			dump_readers.Page(page_id=3, title='Uranus'),
			dump_readers.Page(page_id=4, title='Ant'),
			dump_readers.Page(page_id=5, title='Animals', namespace=14),
			dump_readers.Page(page_id=6, title='Planets', namespace=14),
			]))
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

	cat_qid_to_member_qid = categorization.ConvertCategorylinksToQids(
		categorylinks, pages, entities)

	cat_qid_to_member_qid | 'write' >> WriteToText('/tmp/process.txt')


if __name__ == '__main__':
	p = beam.Pipeline()
	run3(p)
	result = p.run()
	print(result.metrics().query(MetricsFilter()))
