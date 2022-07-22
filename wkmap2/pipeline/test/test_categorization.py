import unittest

from collections.abc import Iterable
from collections import Counter

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, contains_in_any_order
from apache_beam.testing.util import equal_to


import wkmap2.pipeline.categorization as cat
from wkmap2.pipeline.categorization import QidAndIsCat
import wkmap2.pipeline.dump_readers as dump_readers
from wkmap2.pipeline.dag_index import Node, Leaf



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
                    dump_readers.Entity(qid=5, title='Category:Animals'),
                    dump_readers.Entity(qid=6, title='Category:Planets'),
                    dump_readers.Entity(qid=7, title='Category:Things'),
                    dump_readers.Entity(qid=1, title='Beaver'),
                    dump_readers.Entity(qid=2, title='Mercury'),
                    dump_readers.Entity(qid=3, title='Uranus'),
                    dump_readers.Entity(qid=4, title='Ant'),
                    ]))

            output = cat.ConvertCategorylinksToQids(categorylinks, pages, entities)

            assert_that(
              output,
              equal_to([
                  (QidAndIsCat(5, True), QidAndIsCat(1, False)),
                  (QidAndIsCat(5, True), QidAndIsCat(4, False)),
                  (QidAndIsCat(6, True), QidAndIsCat(2, False)),
                  (QidAndIsCat(6, True), QidAndIsCat(3, False)),
                  (QidAndIsCat(7, True), QidAndIsCat(5, True)),
                  (QidAndIsCat(7, True), QidAndIsCat(6, True)),
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
                    dump_readers.Entity(qid=5, title='Category:Animals'),
                    dump_readers.Entity(qid=6, title='Category:Planets'),
                    dump_readers.Entity(qid=7, title='Category:Things'),
                    dump_readers.Entity(qid=1, title='Beaver'),
                    dump_readers.Entity(qid=2, title='Mercury'),
                    dump_readers.Entity(qid=3, title='Uranus'),
                    dump_readers.Entity(qid=4, title='Ant'),
                    ]))
            qranks = (p 
                | 'read qranks' 
                >> beam.Create([
                    dump_readers.QRankEntry(qid=1, qrank=10),
                    dump_readers.QRankEntry(qid=2, qrank=20),
                    dump_readers.QRankEntry(qid=3, qrank=30),
                    dump_readers.QRankEntry(qid=4, qrank=40),
                    ]))

            done, _, _ = cat.CreateCategoryIndex(categorylinks, pages, entities, qranks)

            assert_that(
              done,
              equal_to([
                    Node(node_id=5,
                        top_leaves=[Leaf(4, 40), Leaf(1, 10)],
                        parents={7},
                        children=set(),
                        unprocessed_children=set()),
                    Node(node_id=6,
                        top_leaves=[Leaf(3, 30), Leaf(2, 20)],
                        parents={7},
                        children=set(),
                        unprocessed_children=set()),
                    Node(node_id=7,
                        top_leaves=[Leaf(4, 40), Leaf(3, 30), Leaf(2, 20), Leaf(1, 10)],
                        parents=set(),
                        children={5, 6},
                        unprocessed_children=set()),
              ]))

