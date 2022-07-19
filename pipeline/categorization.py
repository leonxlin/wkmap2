"""Transforms for processing categories."""

from typing import NamedTuple, List, Tuple, TypeVar

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.io import WriteToText
from apache_beam.pvalue import PCollection

from pipeline.dump_readers import Entity, Page, Categorylink, QRankEntry
import pipeline.dag_index as dag_index


class TitleAndIsCat(NamedTuple):
    title: str
    is_cat: bool

class QidAndIsCat(NamedTuple):
    qid: str
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


def CreateCategoryIndex(
    categorylinks: PCollection[Categorylink],
    pages: PCollection[Page],
    entities: PCollection[Entity],
    qranks: PCollection[QRankEntry],
    ):
    raise NotImplementedError
    # TODO: complete

    cat_qid_to_qid = ConvertCategorylinksToQids(categorylinks, pages, entities)

    leaves = (qranks
        | "MakeDagLeaves"
        >> beam.Map(lambda e: dag_index.Leaf(leaf_id=e.qid, score=float(e.qrank))))
    leaf_parent_links = (qranks
        | "MakeDagLeaves"
        >> beam.Map(lambda e: dag_index.Leaf(leaf_id=e.qid, score=float(e.qrank))))
