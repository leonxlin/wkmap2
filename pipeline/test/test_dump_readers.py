import unittest

from pipeline.dump_readers import *

def _count(generator):
    return sum(1 for _ in generator)

class DumpReaderTest(unittest.TestCase):
    
    def test_categorylinks(self):
        reader = CategorylinksDumpReader(
                    'pipeline/testdata/enwiki-20220701-categorylinks-50lines.sql')
        self.assertEqual(_count(reader), 45749)


    def test_page(self):
        reader = PageDumpReader(
                    'pipeline/testdata/enwiki-20220701-page-55lines.sql')
        self.assertEqual(_count(reader), 29436)


    def test_wikidata(self):
        reader = WikidataJsonDumpReader(
                    'pipeline/testdata/wikidata-20220704-all-50lines.json')
        self.assertEqual(_count(reader), 49)


    def test_qrank(self):
        reader = QRankDumpReader(
                    'pipeline/testdata/qrank-1000lines.csv')
        self.assertEqual(_count(reader), 999)


    def test_wikipedia2vec(self):
        reader = Wikipedia2VecDumpReader(
                    'pipeline/testdata/wikipedia2vec_enwiki_20180420_300d-50lines.txt')
        self.assertEqual(_count(reader), 49)


    def test_header_mismatch(self):
        with self.assertRaises(UnexpectedHeaderError):
            for _ in PageDumpReader(
                    'pipeline/testdata/page-changed-schema.sql'):
                pass


