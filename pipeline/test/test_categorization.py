import unittest

from collections.abc import Iterable
from collections import Counter

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, contains_in_any_order
from apache_beam.testing.util import equal_to


import pipeline.categorization as cat
from pipeline.categorization import QidAndIsCat
import pipeline.dump_readers as dump_readers
from pipeline.dag_index import Node, Leaf



def _unordered_equal(a, b):
    return Counter(a) == Counter(b)


# `a` and `b` are (key, value) pairs where the values are iterable and considered
# equal if their elements are the same up to permutation.
def _join_items_equal(a, b):
    if len(a) != 2 or len(b) != 2:
        return False
    return a[0] == b[0] and _unordered_equal(a[1], b[1])



class CategorizationTest(unittest.TestCase):

    def test_convert_categorylinks_to_qids(self):
        with TestPipeline() as p:
            categorylinks = (p 
                | 'read catlinks' 
                >> beam.Create([
                    dump_readers.Categorylink(page_id=1, category='Animals'),
                    dump_readers.Categorylink(page_id=2, category='Planets'),
                    dump_readers.Categorylink(page_id=3, category='Planets'),
                    dump_readers.Categorylink(page_id=4, category='Animals'),
                    dump_readers.Categorylink(page_id=5, category='Things'),
                    dump_readers.Categorylink(page_id=6, category='Things'),
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
                    dump_readers.Page(page_id=7, title='Things', namespace=14),
                    ]))
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

            output = cat.ConvertCategorylinksToQids(categorylinks, pages, entities)

            assert_that(
              output,
              equal_to([
                  (QidAndIsCat('Q5', True), QidAndIsCat('Q1', False)),
                  (QidAndIsCat('Q5', True), QidAndIsCat('Q4', False)),
                  (QidAndIsCat('Q6', True), QidAndIsCat('Q2', False)),
                  (QidAndIsCat('Q6', True), QidAndIsCat('Q3', False)),
                  (QidAndIsCat('Q7', True), QidAndIsCat('Q5', True)),
                  (QidAndIsCat('Q7', True), QidAndIsCat('Q6', True)),
              ]))

    def test_create_category_index(self):
        with TestPipeline() as p:
            categorylinks = (p 
                | 'read catlinks' 
                >> beam.Create([
                    dump_readers.Categorylink(page_id=1, category='Animals'),
                    dump_readers.Categorylink(page_id=2, category='Planets'),
                    dump_readers.Categorylink(page_id=3, category='Planets'),
                    dump_readers.Categorylink(page_id=4, category='Animals'),
                    dump_readers.Categorylink(page_id=5, category='Things'),
                    dump_readers.Categorylink(page_id=6, category='Things'),
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
                    dump_readers.Page(page_id=7, title='Things', namespace=14),
                    ]))
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
            qranks = (p 
                | 'read qranks' 
                >> beam.Create([
                    dump_readers.QRankEntry(qid='Q1', qrank=1),
                    dump_readers.QRankEntry(qid='Q2', qrank=2),
                    dump_readers.QRankEntry(qid='Q3', qrank=3),
                    dump_readers.QRankEntry(qid='Q4', qrank=4),
                    ]))

            done, _, _ = cat.CreateCategoryIndex(categorylinks, pages, entities, qranks)

            assert_that(
              done,
              equal_to([
                    Node(node_id='Q5',
                        top_leaves=[Leaf('Q4', 4), Leaf('Q1', 1)],
                        parents={'Q7'},
                        children=set(),
                        unprocessed_children=set()),
                    Node(node_id='Q6',
                        top_leaves=[Leaf('Q3', 3), Leaf('Q2', 2)],
                        parents={'Q7'},
                        children=set(),
                        unprocessed_children=set()),
                    Node(node_id='Q7',
                        top_leaves=[Leaf('Q4', 4), Leaf('Q3', 3), Leaf('Q2', 2), Leaf('Q1', 1)],
                        parents=set(),
                        children={'Q5', 'Q6'},
                        unprocessed_children=set()),
              ]))

