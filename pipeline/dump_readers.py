"""Simple readers for various Wikipedia/Wikidata-related dump files."""

import re
from itertools import islice
from typing import Optional, NamedTuple, List, Union
import json

from smart_open import open as smart_open, register_compressor

def _verify_header_line(
    template: Union[str, bytes],
    actual: Union[str, bytes],
    escape='<<???>>'):
    """Returns a truthy value if `actual` matches template`.

    Inputs are stripped before comparison. Occurrences of the `escape` string in
    `template` will match any substring.
    """
    assert type(template) == type(actual)
    if type(template) == str:
        wildcard = '.*'
    else:
        assert type(template) == bytes
        if type(escape) == str:
            escape = escape.encode()
        wildcard = b'.*'

    actual = actual.strip()
    template = template.strip()
    template = wildcard.join(re.escape(chunk) for chunk in template.split(escape))
    return re.fullmatch(template, actual)


class UnexpectedHeaderError(Exception):
    pass

def _line_reader(
    path: str, 
    max_lines: Optional[int] = None,
    mode='rb',
    expected_header: str = None):
    """Generator for lines from a compressed or uncompressed file, local or GCS.

    Args:
        max_lines: Maximum number of lines to read, if given.
        expected_header: path to local file containing header template.
    """
    header_lines = []
    if expected_header:
        with open(expected_header, mode=mode) as f:
            header_lines = f.readlines()

    with smart_open(path, mode) as f:
        for i, template in enumerate(header_lines):
            if not _verify_header_line(header_lines[i], f.readline()):
                raise UnexpectedHeaderError(
                    f'The first lines of {path} did not match template '
                    f'{expected_header} at line {i}. Please check whether the '
                    f'file format or schema has changed.')

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


def CategorylinksDumpReader(path: str, max_lines: Optional[int] = None):
    """Reader for MediaWiki `categorylinks` table MySQL dump files.

    Generates Categorylink objects.

    The implementation is hacky: it processes the dump file directly in Python.
    This avoids the time-consuming step of restoring the MySQL database.

    See the schema at https://www.mediawiki.org/wiki/Manual:Categorylinks_table

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _line_reader(
        path, max_lines,
        expected_header='pipeline/dump_headers/categorylinks.sql.template')
    for line in lines:
        for link in _parse_categorylinks_line(line):
            yield link


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
    title: str

    # See https://www.mediawiki.org/wiki/Manual:Namespace.
    namespace: int = 0
    is_redirect: bool = False


def _parse_page_line(line: bytes):
    if not re.match(INSERT_PATTERN, line):
        return
    for match in re.finditer(PAGE_ROW_PATTERN, line):
        yield Page(
                page_id=int(match.group('page_id')),
                title=match.group('page_title').decode('utf-8'),
                namespace=int(match.group('page_namespace')),
                is_redirect=bool(int(match.group('page_is_redirect'))),
              )


def PageDumpReader(path: str, max_lines: Optional[int] = None):
    """Reader for MediaWiki `page` table MySQL dump files.

    Generates Page objects.

    The implementation is hacky: it processes the dump file directly in Python.
    This avoids the time-consuming step of restoring the MySQL database.

    See the schema at https://www.mediawiki.org/wiki/Manual:Page_table

    TODO: consider filtering redirects, talk pages, etc. here.

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _line_reader(
        path, max_lines, 
        expected_header='pipeline/dump_headers/page.sql.template')

    for line in lines:
        for page in _parse_page_line(line):
            yield page



class Entity(NamedTuple):
    """Info extracted about a Wikidata entity."""

    qid: str

    # enwiki sitelink title.
    title: Optional[str] = None

    # English label.
    label: Optional[str] = None

    # Number of sitelinks.
    sitelinks: int = 0

    # English aliases.
    aliases: Optional[List[str]] = None



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
        title=obj['sitelinks'].get('enwiki', {}).get('title'),
        label=obj['labels'].get('en', {}).get('value'),
        aliases=[d['value'] for d in obj['aliases'].get('en', [])],
    )


def WikidataJsonDumpReader(path: str, max_lines: Optional[int] = None):
    """Reader for Wikidata JSON dump files.

    Generates Entity objects.
    
    See https://www.wikidata.org/wiki/Wikidata:Database_download
    and https://doc.wikimedia.org/Wikibase/master/php/md_docs_topics_json.html.

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    for line in _line_reader(path, max_lines, mode='r'):
        for entity in _parse_wikidata_json_line(line):
            yield entity


class QRankEntry(NamedTuple):
    qid: str
    qrank: int


def QRankDumpReader(path: str, max_lines: Optional[int] = None):
    """Reader for QRank files.

    Generates QRankEntry objects.
    
    See https://qrank.wmcloud.org/.

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _line_reader(
        path, max_lines, mode='r', 
        expected_header='pipeline/dump_headers/qrank.csv.template')

    def _parse(line):
        line = line.strip()
        if not line:
            return
        qid, _qrank = line.split(',')
        yield QRankEntry(
            qid=qid,
            qrank=int(_qrank),
        )

    for line in lines:
        for entry in _parse(line):
            yield entry


class Wikipedia2VecEntry(NamedTuple):
    name: str

    # Each entry is either for an entity or a word.
    is_entity: bool

    vector: List[float]


def Wikipedia2VecDumpReader(
    path: str, max_lines: Optional[int] = None, 
    format='word2vec'):
    """Reader for Wikipedia2Vec embedding files.

    Returns a PCollection of Wikipedia2VecEntry objects.

    Currently only the word2vec format is supported.

    See https://wikipedia2vec.github.io/wikipedia2vec/pretrained/
    and https://wikipedia2vec.github.io/wikipedia2vec/commands/#saving-embeddings-in-text-format

    Args:
        path: local or GCS, compressed or uncompressed.
        max_lines: number of lines to truncate at, if present.
    """
    lines = _line_reader(path, max_lines, mode='r')
    entries, dim = map(int, next(lines).strip().split(' '))

    def _parse(line: str):
        line = line.strip()
        if not line:
            return

        tokens = line.split(' ')
        assert len(tokens) == dim + 1

        is_entity = False
        name = tokens[0]
        if tokens[0].startswith('ENTITY/'):
            is_entity = True
            name = tokens[7:]
        
        yield Wikipedia2VecEntry(
            name=name,
            is_entity=is_entity,
            vector=map(float, tokens[1:]),
        )

    for line in lines:
        for entry in _parse(line):
            yield entry


