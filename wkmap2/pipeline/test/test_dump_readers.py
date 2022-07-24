import unittest

import wkmap2.pipeline.dump_readers as dr
import pkg_resources

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.util import equal_to


def _count(generator):
    return sum(1 for _ in generator)

def _testdata_path(filename: str):
    return pkg_resources.resource_filename('wkmap2', f'pipeline/testdata/{filename}')



class DumpReaderTest(unittest.TestCase):
    
    def test_categorylinks(self):
        reader = dr.CategorylinksDumpReader(
                    _testdata_path('enwiki-20220701-categorylinks-50lines.sql'))
        self.assertEqual(_count(reader), 45749)


    def test_page(self):
        reader = dr.PageDumpReader(
                    _testdata_path('enwiki-20220701-page-55lines.sql'))
        self.assertEqual(_count(reader), 29436)


    def test_wikidata(self):
        reader = dr.WikidataJsonDumpReader(
                    _testdata_path('wikidata-20220704-all-50lines.json'))
        entities = list(reader)

        self.assertEqual(len(entities), 49)
        self.assertIn(dr.Entity(
            qid=144,
            title='Dog',
            label='dog',
            sitelinks=295,
            aliases=['domestic dog', 'Canis lupus familiaris', 'Canis familiaris', 'dogs', '🐶', '🐕'],
            claims={31: [55983715], 279: [57814795, 39201]},
        ), entities)


    def test_wikidata_beam(self):
        with TestPipeline() as p:
            entities = p | dr.WikidataJsonDumpReader(
                    _testdata_path('wikidata-20220704-all-50lines.json'))
            output = entities | beam.combiners.Count.Globally()

            assert_that(output, equal_to([49]))


    def test_wikidata_parse_fail(self):
        with TestPipeline() as p:
            entities = p | dr.WikidataJsonDumpReader(
                    _testdata_path('wikidata-bad.json'))
            output = entities | beam.combiners.Count.Globally()

            assert_that(output, equal_to([2]))


    def test_qrank(self):
        reader = dr.QRankDumpReader(
                    _testdata_path('qrank-1000lines.csv'))
        self.assertEqual(_count(reader), 999)


    def test_wikipedia2vec(self):
        reader = dr.Wikipedia2VecDumpReader(
                    _testdata_path('wikipedia2vec_enwiki_20180420_300d-50lines.txt'))
        self.assertEqual(_count(reader), 49)


    def test_header_mismatch(self):
        with self.assertRaises(dr.UnexpectedHeaderError):
            for _ in dr.PageDumpReader(
                    _testdata_path('page-changed-schema.sql')):
                pass


