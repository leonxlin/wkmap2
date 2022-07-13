import unittest

from collections.abc import Iterable
from collections import Counter
from pipeline.categorization import *

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, contains_in_any_order
from apache_beam.testing.util import equal_to





def _unordered_equal(a, b):
    return Counter(a) == Counter(b)


# `a` and `b` are (key, value) pairs where the values are iterable and considered
# equal if their elements are the same up to permutation.
def _join_items_equal(a, b):
    if len(a) != 2 or len(b) != 2:
        return False
    return a[0] == b[0] and _unordered_equal(a[1], b[1])



class DumpReaderTest(unittest.TestCase):
    
    def test_join_pages_and_categorylinks(self):
        with TestPipeline() as p:
            pages = p | "CreatePages" >> beam.Create([
                (0, 'Ant', False), 
                (1, 'Application', False),
                (2, 'Banana', False),
                ]) 

            categorylinks = p | "CreateCls" >> beam.Create([
                (0, 'Animals'), 
                (2, 'Fruits'),
                (0, 'Insects'), 
                ])

            output = JoinPagesAndCategorylinks(pages, categorylinks)

            assert_that(
              output,
              equal_to([
                  ('Ant', ['Animals', 'Insects']),
                  ('Banana', ['Fruits']),
                  ], equals_fn=_join_items_equal)
              )


