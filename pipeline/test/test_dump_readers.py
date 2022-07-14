import unittest

from pipeline.dump_readers import *

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class DumpReaderTest(unittest.TestCase):
    
    def test_categorylinks(self):
        with TestPipeline() as p:
            items = p | CategorylinksDumpReader(
                    'pipeline/testdata/enwiki-20220701-categorylinks-50lines.sql')

            output = items | beam.combiners.Count.Globally()

            assert_that(
              output,
              equal_to([45749]))


    def test_page(self):
        with TestPipeline() as p:
            items = p | PageDumpReader(
                    'pipeline/testdata/enwiki-20220701-page-55lines.sql')

            output = items | beam.combiners.Count.Globally()

            assert_that(
              output,
              equal_to([44660]))


    def test_wikidata(self):
        with TestPipeline() as p:
            items = p | WikidataJsonDumpReader(
                    'pipeline/testdata/wikidata-20220704-all-50lines.json')

            output = items | beam.combiners.Count.Globally()

            assert_that(
              output,
              equal_to([49]))

