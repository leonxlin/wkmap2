"""Beam transforms for indexing the top-scoring N descendants of a DAG node."""

import apache_beam as beam

from typing import NamedTuple, List, Set


LeafId = str
NodeId = str

class Leaf(NamedTuple):
	leaf_id: LeafId
	score: float


class LeafParentLink(NamedTuple):
	leaf_id: LeafId
	parent: NodeId


class NodeLink(NamedTuple):
	child: NodeId
	parent: NodeId


class Node(NamedTuple):
	node_id: NodeId

	# The sorted top-scoring descendant leaves found (so far).
	top_leaves: List[Leaf]

	children: Set[NodeId]
	unprocessed_children: Set[NodeId]
	parents: Set[NodeId]


def _sort_and_truncate_leaves(leaves, N):
	return sorted(leaves, key=lambda leaf: -leaf.score)[:N]


def CreateNodes(leaves, leaf_parent_links, node_links, N=10000):

	leaf_info = {
		'scores': leaves | beam.Map(lambda l: (l.leaf_id, l.score)),
		'leaf_parents': leaf_parent_links | beam.Map(lambda link: (link.leaf_id, link.parent)),
	}

	def _process_join_leaf_info(join_item):
		leaf_id, d = join_item
		if not d['leaf_parents']:
			return
		assert len(d['scores']) == 1
		leaf = Leaf(leaf_id=leaf_id, score=d['scores'][0])
		for parent_id in d['leaf_parents']:
			yield parent_id, leaf

	parent_and_leaf = (leaf_info 
		| "JoinLeafInfo" >> beam.CoGroupByKey() 
		| "ProcessJoinLeafInfo" >> beam.FlatMap(_process_join_leaf_info))

	node_info = {
		'leaves': parent_and_leaf,
		'parents': node_links | "KeyLinksbyChild" >> beam.Map(lambda link: (link.child, link.parent)),
		'children': node_links | "KeyLinksbyParent" >> beam.Map(lambda link: (link.parent, link.child)),
	}

	def _process_join_node_info(join_item):
		node_id, d = join_item
		if not d['leaves'] and not d['children']:
			return

		yield Node(
			node_id = node_id,
			top_leaves = _sort_and_truncate_leaves(d['leaves'], N),
			children = set(d['children']),
			unprocessed_children = set(d['children']),
			parents = set(d['parents']),
			)

	nodes = (node_info 
		| "JoinNodeInfo" >> beam.CoGroupByKey()
		| "ProcessJoinNodeInfo" >> beam.FlatMap(_process_join_node_info))

	return nodes



