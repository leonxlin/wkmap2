"""Transforms for processing categories."""

import apache_beam as beam
from typing import NamedTuple, List
from apache_beam.io import WriteToText

from pipeline.dump_readers import Entity, Page, Categorylink

class TitleAndIsCat(NamedTuple):
    title: str
    is_cat: bool

# See https://www.mediawiki.org/wiki/Manual:Namespace.
_CATEGORY_NAMESPACE = 14

def ReKey(stage_name, a_to_b, a_to_c):
    sources = {
        'b': a_to_b,
        'c': a_to_c,
    }
    def _process_join(join_item):
        a, dic = join_item
        for b in dic['b']:
            for c in dic['c']:
                yield c, b

    return (sources 
        | stage_name + '/Join' >> beam.CoGroupByKey() 
        | stage_name + '/Process' >> beam.FlatMap(_process_join))


def _swap_pair(pair):
    return pair[1], pair[0]


def ConvertCategorylinksToQids(categorylinks, pages, entities):
    def _qids_by_title(e: Entity):
        if not e.title:
            return
        if e.title.startswith('Category:'):
            yield TitleAndIsCat(e.title[9:], True), e.qid
        else:
            yield TitleAndIsCat(e.title, False), e.qid
    qids_by_title = entities | 'GetQidsByTitle' >> beam.FlatMap(_qids_by_title)


    def _page_ids_by_title(p: Page):
        return TitleAndIsCat(p.title, p.namespace == _CATEGORY_NAMESPACE), p.page_id
    page_ids_by_title = pages | 'GetPageIdsByTitle' >> beam.Map(_page_ids_by_title)


    qids_by_page_id = ReKey('GetQidsByPageId', qids_by_title, page_ids_by_title)

    cat_titles_by_page_id = categorylinks | 'GetCatTitlesByPageId' >> beam.Map(lambda cl: (cl.page_id, TitleAndIsCat(cl.category, True)))

    cat_titles_by_member_qid = ReKey('GetCatTitlesByMemberQid', cat_titles_by_page_id, qids_by_page_id)
    member_qid_by_cat_title = cat_titles_by_member_qid | 'GetMemberQidByCatTitle' >> beam.Map(_swap_pair)

    member_qid_by_cat_title | 'WriteMemberQidByCatTitle' >> WriteToText('/tmp/member_qid_by_cat_title.txt')
    qids_by_title | 'WriteQidsByTitle' >> WriteToText('/tmp/qids_by_title.txt')

    member_qid_by_cat_qid = ReKey('GetMemberQidByCatQid', member_qid_by_cat_title, qids_by_title)
    return member_qid_by_cat_qid
