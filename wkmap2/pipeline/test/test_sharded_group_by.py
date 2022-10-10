import unittest

from collections.abc import Iterable
from collections import Counter
from typing import List

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, contains_in_any_order
from apache_beam.testing.util import equal_to, is_empty


from wkmap2.pipeline.sharded_group_by import ShardedGroupByKey



class ShardedGroupByTest(unittest.TestCase):
    def test_sharded_group_by(self):    
        with TestPipeline() as p:
            shardable = p | "CreateShardable" >> beam.Create([
            	(1, 1),
            	(1, 2),
            	(1, 3),
            	(1, 4),
            	(2, 1),
            	(2, 2),
            	(2, 3),
            	(2, 4),
            	(2, 5),
            	(2, 6),
            	(3, 1),
            	(3, 2),
            ]) 
            unshardable = p | "CreateUnshardable" >> beam.Create([
            	(1, 11111),
            	(2, 22222),
            	(2, 222222),
            	(3, 33333),
            	(3, 333333),
            ]) 

            sources = {
            	"shardable": {
            		"shardable": shardable
            	},
            	"unshardable": {
            		"unshardable": unshardable
            	}
            }

            def process_join(join_item):
            	key, d = join_item
            	for v1 in d["shardable"]:
            		for v2 in d["unshardable"]:
            			yield key, v2, v1

            output = (sources 
            	| ShardedGroupByKey(max_items_per_shard=3)
            	| beam.ParDo(process_join))

            assert_that(
                output,
                equal_to([
            	(1, 11111, 1),
            	(1, 11111, 2),
            	(1, 11111, 3),
            	(1, 11111, 4),
            	(2, 22222, 1),
            	(2, 22222, 2),
            	(2, 22222, 3),
            	(2, 22222, 4),
            	(2, 22222, 5),
            	(2, 22222, 6),
            	(2, 222222, 1),
            	(2, 222222, 2),
            	(2, 222222, 3),
            	(2, 222222, 4),
            	(2, 222222, 5),
            	(2, 222222, 6),
            	(3, 33333, 1),
            	(3, 33333, 2),
            	(3, 333333, 1),
            	(3, 333333, 2),
                ]),
              label='done'
            )

