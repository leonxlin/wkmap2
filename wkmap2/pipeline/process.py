"""Example invocations:

python3 -m wkmap2.pipeline.process \
--categorylinks-dump=wkmap2/pipeline/testdata/enwiki-20220701-categorylinks-50lines.sql \
--wikidata-dump=wkmap2/pipeline/testdata/wikidata-20220704-all-50lines.json \
--page-dump=wkmap2/pipeline/testdata/enwiki-20220701-page-55lines.sql

python3 -m wkmap2.pipeline.process \
--task=count_children \
--wikidata-dump=wkmap2/pipeline/testdata/wikidata-20220704-all-50lines.json \
--exclude-class=6256

(Output will be empty as the test data is insufficient.)
"""

import argparse
import hashlib
import json
import logging
import os

from functools import partial
from typing import NamedTuple, List

import apache_beam as beam
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity as DatastoreEntity
from apache_beam.io.gcp.datastore.v1new.types import Key as DatastoreKey
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io import WriteToText
from apache_beam.metrics import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from smart_open import open as smart_open

import wkmap2.pipeline.dump_readers as dump_readers
import wkmap2.pipeline.categorization as categorization


def get_categorylinks(p, args, **kwargs):
    kwargs['max_lines'] = args.max_readlines
    if not args.verify_headers:
        kwargs['expected_header'] = None

    if args.categorylinks_dump:
        return (p
            | 'ReadCategorylinks'
            >> dump_readers.CategorylinksDumpReader(
                args.categorylinks_dump, **kwargs))

    return (p
        | 'CreateCategorylinks'
        >> beam.Create([
            dump_readers.Categorylink(page_id=1, category='Animals'),
            dump_readers.Categorylink(page_id=2, category='Planets'),
            dump_readers.Categorylink(page_id=3, category='Planets'),
            dump_readers.Categorylink(page_id=4, category='Animals'),
            dump_readers.Categorylink(page_id=5, category='Things'),
            dump_readers.Categorylink(page_id=6, category='Things'),
        ]))


def get_pages(p, args, **kwargs):
    kwargs['max_lines'] = args.max_readlines
    if not args.verify_headers:
        kwargs['expected_header'] = None

    if args.page_dump:
        return (p
            | 'ReadPages'
            >> dump_readers.PageDumpReader(
                args.page_dump, **kwargs))
    return (p
        | 'CreatePages'
        >> beam.Create([
            dump_readers.Page(page_id=1, title='Beaver'),
            dump_readers.Page(page_id=2, title='Mercury'),
            dump_readers.Page(page_id=3, title='Uranus'),
            dump_readers.Page(page_id=4, title='Ant'),
            dump_readers.Page(page_id=5, title='Animals', namespace=14),
            dump_readers.Page(page_id=6, title='Planets', namespace=14),
            dump_readers.Page(page_id=7, title='Things', namespace=14),
        ]))


def should_keep_entity(args, entity):
    if not entity.claims or not args.excluded_classes:
        return True
    parents = (
        set(entity.claims.get(dump_readers.INSTANCE_OF_PID, [])) | 
        set(entity.claims.get(dump_readers.SUBCLASS_OF_PID, [])))
    bad_parents = set(args.excluded_classes)
    return not (parents & bad_parents)


def get_entities(p, args, **kwargs):
    kwargs['max_lines'] = args.max_readlines
    if not args.verify_headers:
        kwargs['expected_header'] = None

    if args.wikidata_dump:
        entities = (p
            | 'ReadEntities'
            >> dump_readers.WikidataJsonDumpReader(
                args.wikidata_dump, **kwargs))
    else:
        entities = (p
            | 'CreateEntities'
            >> beam.Create([
                dump_readers.Entity(qid=5, title='Category:Animals', claims={279: [7]}),
                dump_readers.Entity(qid=6, title='Category:Planets', claims={279: [7]}),
                dump_readers.Entity(qid=7, title='Category:Things'),
                dump_readers.Entity(qid=1, title='Beaver', claims={279: [5]}),
                dump_readers.Entity(qid=2, title='Mercury', claims={279: [6, 7]}),
                dump_readers.Entity(qid=3, title='Uranus', claims={279: [6]}),
                dump_readers.Entity(qid=4, title='Ant', claims={279: [5]}),
            ]))

    return entities | "Filter excluded classes" >> beam.Filter(
        partial(should_keep_entity, args))


def get_qranks(p, args, **kwargs):
    kwargs['max_lines'] = args.max_readlines
    if not args.verify_headers:
        kwargs['expected_header'] = None

    if args.qrank_dump:
        return (p
            | 'ReadQRanks'
            >> dump_readers.QRankDumpReader(
                args.qrank_dump, **kwargs))
    return (p
        | 'CreateQRanks'
        >> beam.Create([
            dump_readers.QRankEntry(qid=1, qrank=10),
            dump_readers.QRankEntry(qid=2, qrank=20),
            dump_readers.QRankEntry(qid=3, qrank=30),
            dump_readers.QRankEntry(qid=4, qrank=40),
        ]))


def create_inputs(p, args):
    return (
        get_categorylinks(p, args), 
        get_pages(p, args), 
        get_entities(p, args, require_title=True), 
        get_qranks(p, args))


def get_metrics_str(pipeline):
    ret = {}

    results = pipeline.result.metrics().query(MetricsFilter())
    for key in results:
        ret[key] = []
        for result in results[key]:
            ret[key].append(str(result))
    return json.dumps(ret, indent=4)


class DatastoreEntityWrapper:
    """
    Create a Cloud Datastore entity from a given NamedTuple object.
    """

    def __init__(self, kind: str, id_field: str):
        self.kind = kind
        self.id_field = id_field

    def make_entity(self, nt):
        dic = nt._asdict()
        if self.id_field:
            key = str(dic[self.id_field])
        else:
            key = hashlib.sha1(str(nt).encode('utf-8')).hexdigest()

        entity = DatastoreEntity(DatastoreKey([self.kind, key]))
        entity.set_properties(dic)
        return entity

    def make_entity_stringify(self, nt):
        """For use when `nt._asdict()` can't be passed to `set_properties`."""
        dic = nt._asdict()
        if self.id_field:
            key = str(dic[self.id_field])
        else:
            key = hashlib.sha1(str(nt).encode('utf-8')).hexdigest()

        entity = DatastoreEntity(DatastoreKey([self.kind, key]))
        entity.set_properties({'content': str(nt)})
        return entity


class PageAndCatTitles(NamedTuple):
    page_id: int
    cat_titles: List[str]


def build_categorization_pipeline(p, args):
    categorylinks, pages, entities, qranks = create_inputs(p, args)

    # if args.datastore:
        # # Successful:
        # (pages
        #     | 'PageToDsEntity' >> beam.Map(
        #         DatastoreEntityWrapper('Page', 'page_id').make_entity)
        #     | 'WritePagesToDatastore' >> WriteToDatastore(project))

        # Unsuccessful: sometimes content is too long (longer than 1500 bytes)
        # sources = {
        #     'pages': pages | 'KeyByPageId' >> beam.Map(lambda page: (page.page_id, 1)),
        #     'catlinks': categorylinks | 'KeyCatlinksByPageId' >> beam.Map(lambda link: (link.page_id, link.category)),
        # }
        # def _process_join(join_item):
        #     page_id, dic = join_item
        #     if not dic['pages']:
        #         return
        #     yield PageAndCatTitles(page_id=page_id, cat_titles=sorted(dic['catlinks']))
        # (sources 
        #     | 'JoinForCatlinksToDatastore' >> beam.CoGroupByKey()
        #     | 'ProcessJoinForCatlinksToDatastore' >> beam.FlatMap(_process_join)
        #     | 'PageAndCatTitlesToDsEntity' >> beam.Map(
        #         DatastoreEntityWrapper('PageAndCatTitles', 'page_id').make_entity)
        #     | 'WritePageAndCatTitlesToDatastore' >> WriteToDatastore(project))


    done, pending, ready = categorization.CreateCategoryIndex(
        categorylinks, pages, entities, qranks)
    done | 'WriteOutputDone' >> WriteToText(os.path.join(args.output, 'done_nodes'))
    pending | 'WriteOutputPending' >> WriteToText(os.path.join(args.output, 'pending_nodes'))
    ready | 'WriteOutputReady' >> WriteToText(os.path.join(args.output, 'ready_nodes'))

    # if args.datastore:
    #     (done
    #         | 'DoneNodeToDsEntity' >> beam.Map(
    #             DatastoreEntityWrapper('DoneNode', 'node_id').make_entity_stringify)
    #         | 'WriteDoneNodesToDatastore' >> WriteToDatastore(project))


def build_gather_ancestors_pipeline(p, args):
    entities = get_entities(p, args, require_title=False)

    output = entities | "GatherEntityAncestors" >> categorization.GatherEntityAncestors()
    output | 'WriteOutput' >> WriteToText(os.path.join(args.output, 'gathered_ancestors'))


def build_count_children_pipeline(p, args):
    entities = get_entities(p, args, require_title=False)

    output = entities | "CountChildren" >> categorization.CountChildren(topn=10000)
    output | 'WriteOutput' >> WriteToText(os.path.join(args.output, 'top_parents'))


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--task',
        default='cat',
        type=str,
        help='"cat" or "gather" or "count_children"')
    parser.add_argument(
        '--categorylinks-dump',
        type=str,
        help='Path to copy of enwiki-????????-categorylinks.sql[.gz] (local or GCS).')
    parser.add_argument(
        '--page-dump',
        type=str,
        help='Path to copy of enwiki-????????-page.sql[.gz] (local or GCS).')
    parser.add_argument(
        '--wikidata-dump',
        type=str,
        help='Path to copy of wikidata-????????-all.json[.gz] (local or GCS).')
    parser.add_argument(
        '--qrank-dump',
        type=str,
        help='Path to copy of qrank.csv[.gz] (local or GCS).')
    parser.add_argument(
        '--max-readlines',
        type=int,
        dest='max_readlines',
        default=None,
        help='Maximum number of lines to read from each dumpfile.')
    parser.add_argument(
        '--verify-headers',
        default=True,
        action='store_true',
        help='Verify that dump file "headers" match known schema.')
    parser.add_argument(
        '--no-verify-headers',
        dest='verify_headers',
        action='store_false',
        help='Skip verification of dump file "headers". Use this setting for '
            'sharded dump files.')
    parser.add_argument(
        '--exclude-class',
        type=int,
        nargs='+',
        dest='excluded_classes',
        help='QIDs (ints) to exclude when gathering ancestors. Can specify multiple.')
    parser.add_argument(
        '--datastore',
        default=False,
        action='store_true',
        help='Write some values to datastore.')
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/process_out/',
        help='An output directory.')
    args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    project = pipeline_options.view_as(GoogleCloudOptions).project

    p = None
    with beam.Pipeline(options=pipeline_options) as p:
        if args.task == 'cat':
            build_categorization_pipeline(p, args)
        elif args.task == 'gather':
            build_gather_ancestors_pipeline(p, args)
        elif args.task == 'count_children':
            build_count_children_pipeline(p, args)

    with smart_open(os.path.join(args.output, 'metrics'), 'w') as metrics_file:
        metrics_file.write(get_metrics_str(p))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
