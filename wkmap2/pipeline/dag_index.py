"""Beam transforms for indexing the top-scoring N descendants of a DAG node."""

import logging

import apache_beam as beam
from apache_beam.pvalue import PCollection
from apache_beam.transforms.ptransform import PTransform, ptransform_fn

from typing import NamedTuple, List, Set, Tuple, Iterable, TypeVar, Dict, Optional


LeafId = TypeVar('LeafId', int, str)
NodeId = TypeVar('NodeId', int, str)

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


class Depth(NamedTuple):
    """Information about the link between a node and an ancestor connected by a chain
    of "instance of" and "subclass of" statements.
    """

    # The number of "instance of" links in the chain.
    instances: int = 0

    # The number of "subclass of" links in the chain.
    subclasses: int = 0

    def total(self):
        return self.instances + self.subclasses


def depth_less_than(a: Depth, b: Depth):
    return (a.total(), a.instances) < (b.total(), b.instances)


def combine_depths(a: Depth, b: Depth):
    return Depth(
        instances=a.instances + b.instances,
        subclasses=a.subclasses + b.subclasses)


class NodeWithAncestors(NamedTuple):
    node_id: NodeId

    ancestors: Dict[NodeId, Depth]
    unprocessed_ancestors: Set[NodeId]


def create_node_with_ancestors(node_id: NodeId, instance_of: Optional[Iterable[NodeId]] = None,
    subclass_of: Optional[Iterable[NodeId]] = None):

    ancestors = {}
    if instance_of:
        for a in instance_of:
            ancestors[a] = Depth(instances=1)
    if subclass_of:
        for a in subclass_of:
            ancestors[a] = Depth(subclasses=1)

    return NodeWithAncestors(
        node_id=node_id,
        ancestors=ancestors,
        unprocessed_ancestors=set(ancestors.keys()),
        )


@ptransform_fn
def KeyBy(items: PCollection[NamedTuple], field):
    return items | f"KeyBy_{field}" >> beam.Map(
        lambda item: (getattr(item, field), item))


@ptransform_fn
def GatherAncestors(nodes: PCollection[NodeWithAncestors], iterations=10):
    for iteration in range(iterations):
        nodes = (nodes 
            | f'GatherAncestorsIteration{iteration}' 
            >> RunGatherAncestorsIteration(current_depth = 1 << iteration))
    return nodes


@ptransform_fn
def RunGatherAncestorsIteration(nodes: PCollection[NodeWithAncestors], current_depth: int = 1):
    id_to_node = nodes | 'KeyByNodeId' >> KeyBy('node_id')

    def _emit_unprocessed(node: NodeWithAncestors):
        for ancestor in node.unprocessed_ancestors:
            depth = node.ancestors[ancestor]
            yield ancestor, (node.node_id, depth)

    sources = {
        'node': id_to_node,
        'descendant_and_depth': nodes | 'EmitUnprocessed' >> beam.FlatMap(_emit_unprocessed)
    }

    def _process_join_by_edge_anc(join_item):
        _, dic = join_item
        assert len(dic['node']) == 1

        for anc, anc_depth in dic['node'][0].ancestors.items():
            for desc, desc_depth in dic['descendant_and_depth']:
                # Consider emitting (desc, [all pairs]) instead.
                yield desc, (anc, combine_depths(anc_depth, desc_depth))


    new_links = (sources 
        | 'JoinByEdgeAncestor' >> beam.CoGroupByKey()
        | 'ProcessJoinByEdgeAncestor' >> beam.FlatMap(_process_join_by_edge_anc))

    sources = {
        'node': id_to_node,
        'new_anc_and_depth': new_links
    }

    def _process_join_new_anc(join_item):
        _, dic = join_item
        assert len(dic['node']) == 1

        node = dic['node'][0]
        node.unprocessed_ancestors.clear()

        for ancestor, depth in dic['new_anc_and_depth']:
            if (ancestor not in node.ancestors) or depth_less_than(depth, node.ancestors[ancestor]):
                node.ancestors[ancestor] = depth
            if depth.total() >= 2*current_depth:
                node.unprocessed_ancestors.add(ancestor)

        return node


    return (sources 
        | 'JoinNewAncestors' >> beam.CoGroupByKey()
        | 'ProcessJoinNewAncestors' >> beam.Map(_process_join_new_anc))

