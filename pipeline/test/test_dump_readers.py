import unittest

from pipeline.dump_readers import *

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class DumpReaderTest(unittest.TestCase):
    
    def test_categorylinks(self):
        with TestPipeline() as p:
            titles = p | CategorylinksDumpReader(
                    'pipeline/testdata/enwiki-20220701-categorylinks-50lines.sql')

            output = titles | beam.combiners.Count.Globally()

            assert_that(
              output,
              equal_to([45749]))

