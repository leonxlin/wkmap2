"""Transforms for processing categories."""

from typing import NamedTuple, List, Tuple, TypeVar

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.io import WriteToText
from apache_beam.pvalue import PCollection
from apache_beam.transforms.ptransform import PTransform, ptransform_fn

import wkmap2.pipeline.dump_readers as dump_readers
from wkmap2.pipeline.dump_readers import Entity, Page, Categorylink, QRankEntry
import wkmap2.pipeline.dag_index as dag_index


class TitleAndIsCat(NamedTuple):
    title: str
    is_cat: bool

class QidAndIsCat(NamedTuple):
    qid: int
    is_cat: bool


# See https://www.mediawiki.org/wiki/Manual:Namespace.
_CATEGORY_NAMESPACE = 14


K1 = TypeVar('K1')
K2 = TypeVar('K2')
V = TypeVar('V')

def ReKey(
    stage_name: str,
    k1_to_v: PCollection[Tuple[K1, V]],
    k1_to_k2: PCollection[Tuple[K1, K2]]
    ) -> PCollection[Tuple[K2, V]]:

    sources = {
        'v': k1_to_v,
        'k2': k1_to_k2,
    }
    def _process_join(join_item):
        k1, dic = join_item

        Metrics.distribution(stage_name, 'v_per_k1').update(len(dic['v']))
        Metrics.distribution(stage_name, 'k2_per_k1').update(len(dic['k2']))

        for v in dic['v']:
            for k2 in dic['k2']:
                yield k2, v

    return (sources
        | stage_name + '/Join' >> beam.CoGroupByKey()
        | stage_name + '/Process' >> beam.FlatMap(_process_join))


def _swap_pair(pair):
    return pair[1], pair[0]


def ConvertCategorylinksToQids(
    categorylinks: PCollection[Categorylink],
    pages: PCollection[Page],
    entities: PCollection[Entity],
    write_intermediates=False
    ) -> PCollection[Tuple[QidAndIsCat, QidAndIsCat]]:

    def _title_to_qid(e: Entity):
        if not e.title:
            return
        if e.title.startswith('Category:'):
            Metrics.counter('GetTitleToQid', 'categories').inc()
            yield TitleAndIsCat(e.title[9:], True), QidAndIsCat(e.qid, True)
        else:
            Metrics.counter('GetTitleToQid', 'non_categories').inc()
            yield TitleAndIsCat(e.title, False), QidAndIsCat(e.qid, False)
    title_to_qid = entities | 'GetTitleToQid' >> beam.FlatMap(_title_to_qid)

    def _title_to_page_id(p: Page):
        return TitleAndIsCat(p.title, p.namespace == _CATEGORY_NAMESPACE), p.page_id
    title_to_page_id = pages | 'GetTitleToPageId' >> beam.Map(_title_to_page_id)

    page_id_to_qid = ReKey('GetPageIdToQid', title_to_qid, title_to_page_id)

    page_id_to_cat_title = (categorylinks
        | 'GetPageIdToCatTitle'
        >> beam.Map(lambda cl: (cl.page_id, TitleAndIsCat(cl.category, True))))

    qid_to_cat_title = ReKey('GetQidToCatTitle', page_id_to_cat_title, page_id_to_qid)
    cat_title_to_qid = qid_to_cat_title | 'GetCatTitleToQid' >> beam.Map(_swap_pair)

    if write_intermediates:
        cat_title_to_qid | 'WriteCatTitleToQid' >> WriteToText('/tmp/cat_title_to_qid.txt')
        title_to_qid | 'WriteTitleToQid' >> WriteToText('/tmp/title_to_qid.txt')

    return ReKey('GetCatQidToQid', cat_title_to_qid, title_to_qid)


class _MakeDagLinksFromQidPairs(beam.DoFn):
    """Outputs dag_index.LeafParentLink and dag_index.NodeLink objects."""
    def process(self, qid_pair: Tuple[QidAndIsCat, QidAndIsCat]):
        parent, child = qid_pair

        if child.is_cat:
            yield beam.pvalue.TaggedOutput('node_link',
                dag_index.NodeLink(child=child.qid, parent=parent.qid))
        else:
            yield beam.pvalue.TaggedOutput('leaf_parent_link',
                dag_index.LeafParentLink(leaf_id=child.qid, parent=parent.qid))



def CreateCategoryIndex(
    categorylinks: PCollection[Categorylink],
    pages: PCollection[Page],
    entities: PCollection[Entity],
    qranks: PCollection[QRankEntry],
    ) -> Tuple[PCollection[dag_index.Node], PCollection[dag_index.Node], PCollection[dag_index.Node]]:
    """Returns done, ready, pending PCollections."""

    cat_qid_to_qid = ConvertCategorylinksToQids(categorylinks, pages, entities)

    leaves = (qranks
        | "MakeDagLeaves"
        >> beam.Map(lambda e: dag_index.Leaf(leaf_id=e.qid, score=float(e.qrank))))
    node_links, leaf_parent_links = (cat_qid_to_qid
        | "MakeDagLinksFromQidPairs"
        >> beam.ParDo(_MakeDagLinksFromQidPairs()).with_outputs(
            "node_link", "leaf_parent_link"))

    return dag_index.CreateIndex(leaves, leaf_parent_links, node_links)


@ptransform_fn
def GatherEntityAncestors(entities: PCollection[Entity]) -> PCollection[dag_index.NodeWithAncestors]:
    def init_node_with_ancestors(entity: Entity):
        if not entity.claims:
            return dag_index.create_node_with_ancestors(entity.qid)
        return dag_index.create_node_with_ancestors(
            entity.qid,
            instance_of=entity.claims.get(dump_readers.INSTANCE_OF_PID),
            subclass_of=entity.claims.get(dump_readers.SUBCLASS_OF_PID)
            )

    nodes = entities | 'InitNodeWithAncestors' >> beam.Map(init_node_with_ancestors)
    return nodes | 'GatherAncestors' >> dag_index.GatherAncestors()


