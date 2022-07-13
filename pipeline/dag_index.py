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


def _sorted_and_truncated(leaves, N):
	return sorted(set(leaves), key=lambda leaf: -leaf.score)[:N]


def CreateNodes(leaves, leaf_parent_links, node_links, N=10000):

	leaf_info = {
		'scores': leaves | beam.Map(lambda l: (l.leaf_id, l.score)),
		'parents': leaf_parent_links | beam.Map(lambda link: (link.leaf_id, link.parent)),
	}

	def _process_join_leaf_info(join_item):
		leaf_id, d = join_item
		if not d['parents']:
			return
		assert len(d['scores']) == 1
		leaf = Leaf(leaf_id=leaf_id, score=d['scores'][0])
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


def CreateIndex(leaves, leaf_parent_links, node_links, N=10000):
	nodes = CreateNodes(leaves, leaf_parent_links, node_links, N)

	for i in range(5):
		nodes = _PropagateLeavesOnce("S" + str(i), nodes, N)

	return nodes

def _PropagateLeavesOnce(stage_name: str, nodes, N: int):
	def _generate_parent_child_pairs(node: Node):
		if node.unprocessed_children:
			# Don't propagate yet; more leaves may show up later.
			return

		for parent in node.parents:
			yield parent, node

	parent_info = {
		'child_nodes': (nodes 
			| (stage_name + "/KeyNodesByParent") >> beam.FlatMap(_generate_parent_child_pairs)),
		'nodes': (nodes 
			| (stage_name + "/KeyNodesById") >> beam.Map(lambda n: (n.node_id, n))),
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
		| (stage_name + "/JoinByParent") >> beam.CoGroupByKey()
		| (stage_name + "/ProcessJoinbyParent") >> beam.Map(_process_join_parent))


