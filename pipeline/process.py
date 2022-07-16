
import apache_beam as beam
from apache_beam.io import WriteToText
import dump_readers as dump_readers
import categorization as categorization


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
	squares = p | 'MakeSquares' >> beam.Create([((1, 1), 1), ((3, 3), 9), ((2, 2), 4), ((8, 8), 64)])
	num_names = p | 'MakeNumNames' >> beam.Create([((1, 1), 'one'), ((3, 3), 'three'), ((2, 2), 'two'), ((8, 8), 'eight')])
	squares_by_num_name = categorization.ReKey('ReKey', squares, num_names)
	squares_by_num_name | beam.Map(print)



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
	with beam.Pipeline() as p:
		run(p)