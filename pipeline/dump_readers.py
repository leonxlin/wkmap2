"""Beam transforms for reading from various dump files."""

import re
from itertools import islice
from typing import Optional, NamedTuple, List
import json

import apache_beam as beam
from smart_open import open as smart_open, register_compressor


def smart_line_reader(path: str, max_lines: Optional[int] = None, mode='rb'):
    """Generator for lines from a compressed or uncompressed file,
    local or GCS. Truncates at `max_lines` if given."""
    with smart_open(path, mode) as f:
        for i, line in enumerate(f):
            yield line
            if max_lines and (i >= max_lines):
                return

INSERT_PATTERN=re.compile(rb'INSERT INTO `.*` VALUES')


# Based on the schema at 
# https://www.mediawiki.org/wiki/Manual:Categorylinks_table
#
# See unit tests for example data. 
CATEGORYLINKS_ROW_PATTERN=re.compile(
        rb'\('
        rb'(?P<cl_from>\d+),'
        rb"'(?P<cl_to>.*?)',"
        rb"'(?P<cl_sortkey>.*?)',"
        rb"'(?P<cl_timestamp>.*?)',"
        rb"'(?P<cl_sortkey_prefix>.*?)',"
        rb"'(?P<cl_collation>.*?)',"
        rb"'(?P<cl_type>.*?)'"
        rb'\)'
        )


class Categorylink(NamedTuple):
    page_id: int
    category: str


def _parse_categorylinks_line(line: bytes):
    if not re.match(INSERT_PATTERN, line):
        return
    for match in re.finditer(CATEGORYLINKS_ROW_PATTERN, line):
        yield Categorylink(
                page_id=int(match.group('cl_from')), 
                category=match.group('cl_to').decode('utf-8'),
              )


@beam.ptransform_fn
def CategorylinksDumpReader(_, path: str, max_lines: Optional[int] = None):
    """Reader for MediaWiki `categorylinks` table MySQL dump files.

    Returns a PCollection of Categorylink objects.

    The implementation is hacky: it processes the dump file directly in Python
    and assumes the schema / dumpfile format will not change. This avoids the 
    time-consuming step of restoring the MySQL database.

    See the schema at https://www.mediawiki.org/wiki/Manual:Categorylinks_table

    TODO: verify schema.

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _ | 'ReadCategorylinksDump' >> beam.Create(
            smart_line_reader(path, max_lines))

    return lines | 'ParseCategorylinksDumpLine' >> beam.FlatMap(
            _parse_categorylinks_line)


# Based on the schema at 
# https://www.mediawiki.org/wiki/Manual:Page_table
#
# See unit tests for example data. 
PAGE_ROW_PATTERN=re.compile(
        rb'\('
        rb'(?P<page_id>\d+),'
        rb'(?P<page_namespace>\d+),'
        rb"'(?P<page_title>.*?)',"
        rb"(?P<page_is_redirect>\d+),"
  		rb"(?P<page_is_new>.*?),"
  		rb"(?P<page_random>.*?),"
  		rb"(?P<page_touched>.*?),"
  		rb"(?P<page_links_updated>.*?),"
  		rb"(?P<page_latest>\d+),"
  		rb"(?P<page_len>\d+),"
  		rb"(?P<page_content_model>.*?),"
  		rb"(?P<page_lang>.*?)"
        rb'\)'
        )


class Page(NamedTuple):
    page_id: int
    namespace: int
    title: str
    is_redirect: bool


def _parse_page_line(line: bytes):
    if not re.match(INSERT_PATTERN, line):
        return
    for match in re.finditer(PAGE_ROW_PATTERN, line):
        yield Page(
                page_id=int(match.group('page_id')),
                namespace=int(match.group('page_namespace')),
                title=match.group('page_title').decode('utf-8'),
                is_redirect=bool(int(match.group('page_is_redirect'))),
              )


@beam.ptransform_fn
def PageDumpReader(_, path: str, max_lines: Optional[int] = None):
    """Reader for MediaWiki `page` table MySQL dump files.

    Returns a PCollection of Page objects.

    The implementation is hacky: it processes the dump file directly in Python
    and assumes the schema / dumpfile format will not change. This avoids the
    time-consuming step of restoring the MySQL database.

    See the schema at https://www.mediawiki.org/wiki/Manual:Page_table

    TODO: consider filtering redirects, talk pages, etc. here.
    TODO: verify schema.

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _ | 'ReadPageDump' >> beam.Create(
            smart_line_reader(path, max_lines))

    return lines | 'ParsePageDumpLine' >> beam.FlatMap(
            _parse_page_line)



class Entity(NamedTuple):
    """Info extracted about a Wikidata entity."""

    qid: str

    # enwiki sitelink title.
    title: Optional[str]

    # Number of sitelinks.
    sitelinks: int

    # English aliases.
    aliases: List[str]



def _parse_wikidata_json_line(line: str):
    # The JSON dumps look like
    # [
    # {...},
    # {...},
    # ...
    # ]
    line = line.strip()
    if line.endswith(','):
        line = line[:-1]
    if line in ('[', ']'):
        return

    obj = json.loads(line)
    yield Entity(
        qid=obj['id'],
        sitelinks=len(obj['sitelinks']),
        title=obj['labels'].get('en')['value'],
        aliases=[d['value'] for d in obj['aliases'].get('en', [])],
    )


@beam.ptransform_fn
def WikidataJsonDumpReader(_, path: str, max_lines: Optional[int] = None):
    """Reader for Wikidata JSON dump files.

    Returns a PCollection of Entity objects.
    
    See https://www.wikidata.org/wiki/Wikidata:Database_download
    and https://doc.wikimedia.org/Wikibase/master/php/md_docs_topics_json.html.

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _ | 'ReadWikidataJsonDump' >> beam.Create(
            smart_line_reader(path, max_lines, mode='r'))

    return lines | 'ParseWikidataJsonDumpLine' >> beam.FlatMap(
            _parse_wikidata_json_line)


class QRankEntry(NamedTuple):
    qid: str
    qrank: int


def _parse_qrank_line(line: str):
    line = line.strip()
    if line == 'Entity,QRank':
        return

    qid, _qrank = line.split(',')
    yield QRankEntry(
        qid=qid,
        qrank=int(_qrank),
    )


@beam.ptransform_fn
def QRankDumpReader(_, path: str, max_lines: Optional[int] = None):
    """Reader for QRank files.

    Returns a PCollection of QRankEntry objects.
    
    See https://qrank.wmcloud.org/.

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _ | 'ReadQRankDump' >> beam.Create(
            smart_line_reader(path, max_lines, mode='r'))

    return lines | 'ParseQRankDumpLine' >> beam.FlatMap(
            _parse_qrank_line)

