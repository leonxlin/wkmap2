import unittest

from collections.abc import Iterable
from collections import Counter

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, contains_in_any_order
from apache_beam.testing.util import equal_to


from pipeline.categorization import *
import pipeline.dump_readers as dump_readers



def _unordered_equal(a, b):
    return Counter(a) == Counter(b)


# `a` and `b` are (key, value) pairs where the values are iterable and considered
# equal if their elements are the same up to permutation.
def _join_items_equal(a, b):
    if len(a) != 2 or len(b) != 2:
        return False
    return a[0] == b[0] and _unordered_equal(a[1], b[1])



class CategorizationTest(unittest.TestCase):

    def test_convert_categorlinks_to_qids(self):
        with TestPipeline() as p:
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

            output = ConvertCategorylinksToQids(categorylinks, pages, entities)

            assert_that(
              output,
              equal_to([
                  ('Q5', 'Q1'),
                  ('Q5', 'Q4'),
                  ('Q6', 'Q2'),
                  ('Q6', 'Q3'),
              ]))

