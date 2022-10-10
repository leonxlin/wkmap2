"""Sharded group by util to avoid performance problems related to hotkeys."""

import random

import apache_beam as beam
import apache_beam.pvalue as pvalue
from apache_beam.pvalue import PCollection
from apache_beam.transforms.ptransform import PTransform, ptransform_fn
from apache_beam.transforms.util import Reshuffle

from typing import Any, NamedTuple, List, Set, Tuple, Iterable, TypeVar, Dict, Optional



K = TypeVar('K')


class ShardedKey(NamedTuple):
    key: K
    shard: int


@ptransform_fn
def ShardedGroupBy(
	sources: Dict[str, Dict[str, PCollection[Tuple[K, Any]]]], 
	max_items_per_shard=10000):
	"""
	Alternative to beam.CoGroupByKey() where one source can be "shardable".
	The output is a PCollection of (key, value) pairs where some keys may be
	duplicated.

	Args:
	sources: dict with two keys, "shardable" and "unshardable"
	"""

	assert len(sources["shardable"]) == 1
	shardable_name, shardable_pcoll = sources["shardable"].popitem()
	unshardable = sources["unshardable"]
	assert unshardable
	assert shardable_name not in unshardable

	def get_num_shards(item: Tuple[K, int], max_items_per_shard=max_items_per_shard):
		key, count = item
		if count >= max_items_per_shard:
			yield key, (count // max_items_per_shard) + 1 

	num_shards_per_key = pvalue.AsDict(shardable_pcoll 
		| "CountItemsPerKey" >> beam.transforms.combiners.Count.PerKey()
		| "GetNumShards" >> beam.ParDo(get_num_shards))

	def shard(item: Tuple[K, Any], num_shards_per_key: Dict[K, int], duplicate=False):
		key, value = item
		n_shards = num_shards_per_key.get(key, 1)
		if duplicate:
			for i in range(n_shards):
				yield ShardedKey(key=key, shard=i), value
		else:
			yield ShardedKey(key=key, shard=random.randrange(0, n_shards)), value

	sharded_sources = {}

	sharded_sources[shardable_name] = (shardable_pcoll 
		| "ShardShardableInput" >> beam.ParDo(
			shard, 
			num_shards_per_key=num_shards_per_key, 
			duplicate=False))

	for name, pcoll in unshardable.items():
		sharded_sources[name] = (pcoll 
			| f"ShardUnshardableInput_{name}" >> beam.ParDo(shard,
				num_shards_per_key=num_shards_per_key,
				duplicate=True)
			)

	def unshard_keys(item: Tuple[ShardedKey, Any]):
		sk, val = item
		yield sk.key, val

	return (sharded_sources 
		| "ShardedCoGroupByKey" >> beam.CoGroupByKey()
		| "UnshardKeys" >> beam.ParDo(unshard_keys))
