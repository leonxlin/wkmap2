import unittest

from collections.abc import Iterable
from collections import Counter
from pipeline.dag_index import *

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, contains_in_any_order
from apache_beam.testing.util import equal_to


class DagIndexTest(unittest.TestCase):
    def test_create_index(self):    
        with TestPipeline() as p:
            leaves = p | "CreateLeaves" >> beam.Create([
                Leaf('l0', 5),
                Leaf('l1', 9),
                Leaf('l2', 8),
                Leaf('l3', 2),
                ]) 

            leaf_parent_links = p | "CreateLeafParentLinks" >> beam.Create([
                LeafParentLink('l0', 'n0'),
                LeafParentLink('l1', 'n1'),
                LeafParentLink('l2', 'n0'),
                LeafParentLink('l3', 'n1'),
                ])

            node_links = p | "CreateNodeLinks" >> beam.Create([
                NodeLink('n0', 'n2'),
                NodeLink('n1', 'n2'),
                NodeLink('n2', 'n3'),
                ])

            output = CreateIndex(leaves, leaf_parent_links, node_links)

            assert_that(
              output,
              equal_to([
                Node(node_id='n0',
                    top_leaves=[Leaf('l2', 8), Leaf('l0', 5)],
                    parents={'n2'},
                    children=set(),
                    unprocessed_children=set()),
                Node(node_id='n1',
                    top_leaves=[Leaf('l1', 9), Leaf('l3', 2)],
                    parents={'n2'},
                    children=set(),
                    unprocessed_children=set()),
                Node(node_id='n2',
                    top_leaves=[Leaf('l1', 9), Leaf('l2', 8), Leaf('l0', 5), Leaf('l3', 2)],
                    parents={'n3'},
                    children={'n0', 'n1'},
                    unprocessed_children=set()),
                Node(node_id='n3',
                    top_leaves=[Leaf('l1', 9), Leaf('l2', 8), Leaf('l0', 5), Leaf('l3', 2)],
                    parents=set(),
                    children={'n2'},
                    unprocessed_children=set()),
                  ]),
              )


    
    def test_create_nodes(self):
        with TestPipeline() as p:
            leaves = p | "CreateLeaves" >> beam.Create([
                Leaf('l0', 5),
                Leaf('l1', 9),
                Leaf('l2', 8),
                Leaf('l3', 2),
                ]) 

            leaf_parent_links = p | "CreateLeafParentLinks" >> beam.Create([
                LeafParentLink('l0', 'n0'),
                LeafParentLink('l1', 'n1'),
                LeafParentLink('l2', 'n0'),
                LeafParentLink('l3', 'n1'),
                ])

            node_links = p | "CreateNodeLinks" >> beam.Create([
                NodeLink('n0', 'n2'),
                NodeLink('n1', 'n2'),
                NodeLink('n2', 'n3'),
                ])

            output = CreateNodes(leaves, leaf_parent_links, node_links)

            assert_that(
              output,
              equal_to([
                Node(node_id='n0',
                    top_leaves=[Leaf('l2', 8), Leaf('l0', 5)],
                    parents={'n2'},
                    children=set(),
                    unprocessed_children=set()),
                Node(node_id='n1',
                    top_leaves=[Leaf('l1', 9), Leaf('l3', 2)],
                    parents={'n2'},
                    children=set(),
                    unprocessed_children=set()),
                Node(node_id='n2',
                    top_leaves=[],
                    parents={'n3'},
                    children={'n0', 'n1'},
                    unprocessed_children={'n0', 'n1'}),
                Node(node_id='n3',
                    top_leaves=[],
                    parents=set(),
                    children={'n2'},
                    unprocessed_children={'n2'}),
                  ]),
              )
