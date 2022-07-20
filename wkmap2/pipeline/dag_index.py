"""Beam transforms for indexing the top-scoring N descendants of a DAG node."""

import logging

import apache_beam as beam
from apache_beam.pvalue import PCollection

from typing import NamedTuple, List, Set, Tuple, Iterable


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


def _sorted_and_truncated(leaves: Iterable[Leaf], N: int):
	return sorted(set(leaves), key=lambda leaf: -leaf.score)[:N]


def CreateNodes(
	leaves: PCollection[Leaf],
	leaf_parent_links: PCollection[LeafParentLink],
	node_links: PCollection[NodeLink], 
	N=10000) -> PCollection[Node]:

	leaf_info = {
		'scores': leaves | beam.Map(lambda l: (l.leaf_id, l.score)),
		'parents': leaf_parent_links | beam.Map(lambda link: (link.leaf_id, link.parent)),
	}

	def _process_join_leaf_info(join_item):
		leaf_id, d = join_item
		if not d['parents']:
			return

		score = 0
		if d['scores']:
			score = d['scores'][0]
			if len(d['scores']) > 1:
				logging.warning(
					f'More than 1 score found for leaf_id '
					f'{leaf_id}: {d["scores"]}')

		leaf = Leaf(leaf_id=leaf_id, score=score)
		for parent_id in d['parents']:
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
			top_leaves = _sorted_and_truncated(d['leaves'], N),
			children = set(d['children']),
			unprocessed_children = set(d['children']),
			parents = set(d['parents']),
			)

	nodes = (node_info 
		| "JoinNodeInfo" >> beam.CoGroupByKey()
		| "ProcessJoinNodeInfo" >> beam.FlatMap(_process_join_node_info))

	return nodes


def CreateIndex(
	leaves: PCollection[Leaf],
	leaf_parent_links: PCollection[LeafParentLink],
	node_links: PCollection[NodeLink], 
	iters=10, N=10000):
	nodes = CreateNodes(leaves, leaf_parent_links, node_links, N)

	# Split nodes based on whether they are ready to propagate leaves upward.
	pending, ready = (nodes | "SplitReadyNodes" 
		>> beam.ParDo(_SplitReadyNodes()).with_outputs('pending', 'ready'))
	done = []

	for i in range(iters):
		done.append(ready)
		pending, ready = _PropagateLeavesOnce(ready, pending, 
			stage_name=f"PropagateLeavesIter{i}", N=N)

	return (done | beam.Flatten()), pending, ready


class _SplitReadyNodes(beam.DoFn):	
	"""Separates nodes based on whether they are ready to propagate their leaves upward."""
	def process(self, node: Node):
		if node.unprocessed_children:
			yield beam.pvalue.TaggedOutput('pending', node)
		else:
			yield beam.pvalue.TaggedOutput('ready', node)


def _PropagateLeavesOnce(
	giving_nodes: PCollection[Node], 
	receiving_nodes: PCollection[Node], 
	stage_name: str = "", N=10000) -> Tuple[PCollection[Node], PCollection[Node]]:

	def _generate_parent_child_pairs(node: Node):
		for parent in node.parents:
			yield parent, node

	parent_info = {
		'child_nodes': (giving_nodes 
			| (stage_name + "/KeyGivingNodesByParent") 
			>> beam.FlatMap(_generate_parent_child_pairs)),
		'nodes': (receiving_nodes 
			| (stage_name + "/KeyReceivingNodesById")
			>> beam.Map(lambda n: (n.node_id, n))),
	}

	def _process_join_parent(join_item):		
		node_id, d = join_item

		assert len(d['nodes']) == 1
		node = d['nodes'][0]

		for child_node in d['child_nodes']:
			if child_node.node_id in node.unprocessed_children:
				node.top_leaves.extend(child_node.top_leaves)
				node.unprocessed_children.remove(child_node.node_id)

		sorted_leaves = _sorted_and_truncated(node.top_leaves, N)
		node.top_leaves.clear()
		node.top_leaves.extend(sorted_leaves)
		return node

	return (parent_info 
		| (stage_name + "/JoinByParent") 
			>> beam.CoGroupByKey()
		| (stage_name + "/ProcessJoinByParent") 
			>> beam.Map(_process_join_parent)
		| (stage_name + "/SplitReadyNodes")
			>> beam.ParDo(_SplitReadyNodes()).with_outputs("pending", "ready"))


