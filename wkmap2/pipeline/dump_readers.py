"""Simple readers for various Wikipedia/Wikidata-related dump files."""

import logging
import re
import glob
from itertools import islice
from typing import Optional, NamedTuple, List, Union, AnyStr, Type, TypeVar, Iterator, Iterable, Generic
import json
import traceback
import pkg_resources

import apache_beam as beam
from apache_beam.io.textio import ReadAllFromText
from apache_beam.transforms.ptransform import PTransform, ptransform_fn
from apache_beam.transforms.util import Reshuffle

from smart_open import open as smart_open

import wkmap2.pipeline.gcs_glob as gcs_glob


def local_or_gcs_glob(pattern: str) -> Iterable[str]:
    if gcs_glob.parse_gcs_path(pattern):
        return gcs_glob.glob(pattern)
    return glob.glob(pattern)


@ptransform_fn
def GlobToLines(
    pipeline,
    pattern: str,
    verbose=True,
    read_type: Type[AnyStr] = str):
    """Returns a collection of lines from files matching the given pattern.

    Args:
        pattern: glob path, local or gcs, compressed or uncompressed.
    """
    filenames = local_or_gcs_glob(pattern)

    logging.info(f'Found {len(filenames)} files matching {pattern}.')


def _header_path(filename: str):
    return pkg_resources.resource_filename('wkmap2', f'pipeline/dump_headers/{filename}')


def _verify_header_line(
    template: AnyStr,
    actual: AnyStr):
    """Returns a truthy value if `actual` matches template`.

    Inputs are stripped before comparison. Occurrences of the `escape` string in
    `template` will match any substring.
    """
    if isinstance(template, str):
        escape: AnyStr = '<<???>>'
        wildcard: AnyStr = '.*'
    else:
        escape = b'<<???>>'
        wildcard = b'.*'

    actual = actual.strip()
    template = template.strip()
    template = wildcard.join(re.escape(chunk) for chunk in template.split(escape))
    return re.fullmatch(template, actual)


class UnexpectedHeaderError(Exception):
    pass


def _verify_header(expected_header_file: str, dump_file: str, mode: str):
    header_lines = []
    with open(expected_header_file, mode=mode) as f:
        header_lines = f.readlines()

    with smart_open(dump_file, mode) as f:
        for i, template in enumerate(header_lines):
            if not _verify_header_line(header_lines[i], f.readline()):
                raise UnexpectedHeaderError(
                    f'The first lines of {dump_file} did not match template '
                    f'{expected_header_file} at line {i}. Please check '
                    f'whether the file format or schema has changed.')


class NoFilesMatchedError(Exception):
    pass


R = TypeVar('R')
class DumpReader(PTransform, Generic[AnyStr, R], Iterable[R]):
    """Reads lines from files as a PCollection or iterator."""

    def __init__(self,
        pattern: str,
        read_type: Type[AnyStr] = str,
        expected_header: Optional[str] = None,
        max_lines: int = None):
        """
        Args:
            pattern: glob pattern to match. Local or GCS, compressed or uncompressed.
            read_type: str or bytes.
            expected_header: File to match dump's initial lines against.
            max_lines: maximum number of lines to read.
        """

        # super(DumpReader, self).__init__()
        PTransform.__init__(self)
        self.pattern = pattern
        self.read_type = read_type
        self.read_mode = 'r' if read_type is str else 'rb'
        self.filenames = local_or_gcs_glob(pattern)
        self.max_lines = max_lines

        if not self.filenames:
            raise NoFilesMatchedError(f'No files matched {pattern}')
        logging.info(f'Found {len(self.filenames)} files matching {pattern}.')

        if expected_header:
            _verify_header(expected_header, self.filenames[0], self.read_mode)


    def parse_line(self, line: AnyStr) -> Iterator[R]:
        # This function should be overridden by child classes.
        yield line


    def parse_line_wrapper(self, line: AnyStr):
        try:
            for obj in self.parse_line(line):
                yield obj
        except Exception:
            logging.warning(f"Could not parse the following line: {line}")
            logging.warning(traceback.format_exc())
            return


    def expand(self, pipeline):
        if self.read_type == bytes:
            coder = beam.coders.coders.BytesCoder()
        elif self.read_type == str:
            coder = beam.coders.coders.StrUtf8Coder()

        return (pipeline
            | beam.Create(self.filenames)
            | Reshuffle()
            | ReadAllFromText(coder=coder)
            | beam.FlatMap(self.parse_line_wrapper))


    def __iter__(self) -> Iterator[R]:
        """Does not skip header lines."""

        def read():
            i = 0
            for filename in self.filenames:
                with smart_open(filename, self.read_mode) as f:
                    for line in f:
                        for obj in self.parse_line(line):
                            yield obj
                        i += 1
                        if self.max_lines and i >= self.max_lines:
                            return

        return read()




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

    # 'page', 'subcat', or 'file'. See schema.
    cl_type: str = 'page'


class CategorylinksDumpReader(DumpReader):
    """Reader for MediaWiki `categorylinks` table MySQL dump files.

    Parses lines into Categorylink objects.

    This is somewhat hacky: we process the dump file directly in Python.
    This avoids the time-consuming step of restoring the MySQL database.

    See the schema at https://www.mediawiki.org/wiki/Manual:Categorylinks_table
    """

    def __init__(self, pattern: str, filter_to_types=('page', 'subcat'),
        expected_header=_header_path('categorylinks.sql.template'), **kwargs):
        DumpReader.__init__(self,
            pattern=pattern,
            read_type=bytes,
            expected_header=expected_header,
            **kwargs)

        self.filter_to_types = filter_to_types

    def parse_line(self, line: bytes) -> Iterator[Categorylink]:
        if not re.match(INSERT_PATTERN, line):
            return
        for match in re.finditer(CATEGORYLINKS_ROW_PATTERN, line):
            link = Categorylink(
                    page_id=int(match.group('cl_from')),
                    category=match.group('cl_to').decode('utf-8'),
                    cl_type=match.group('cl_type').decode('utf-8'),
                  )
            if self.filter_to_types and (
                link.cl_type not in self.filter_to_types):
                continue
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


class PageDumpReader(DumpReader):
    """Reader for MediaWiki `page` table MySQL dump files.

    Parses lines into Page objects.

    This is somewhat hacky: we process the dump file directly in Python.
    This avoids the time-consuming step of restoring the MySQL database.

    See the schema at https://www.mediawiki.org/wiki/Manual:Page_table
    """

    def __init__(self,
        pattern: str,
        filter_to_namespaces=(0, 14),
        drop_redirects=True,
        expected_header=_header_path('page.sql.template'),
        **kwargs):
        DumpReader.__init__(self,
            pattern=pattern,
            read_type=bytes,
            expected_header=expected_header,
            **kwargs)

        self.filter_to_namespaces = filter_to_namespaces
        self.drop_redirects = drop_redirects

    def parse_line(self, line: bytes) -> Iterator[Page]:
        if not re.match(INSERT_PATTERN, line):
            return
        for match in re.finditer(PAGE_ROW_PATTERN, line):
            page = Page(
                    page_id=int(match.group('page_id')),
                    title=match.group('page_title').decode('utf-8'),
                    namespace=int(match.group('page_namespace')),
                    is_redirect=bool(int(match.group('page_is_redirect'))),
                  )

            if self.filter_to_namespaces and (
                page.namespace not in self.filter_to_namespaces):
                continue
            if self.drop_redirects and page.is_redirect:
                continue

            yield page


class Entity(NamedTuple):
    """Info extracted about a Wikidata entity."""

    qid: str

    # enwiki sitelink title, with underscores instead of spaces.
    # Namespace prefix (e.g., 'Category:') included.
    title: Optional[str] = None

    # English label.
    label: Optional[str] = None

    # Number of sitelinks.
    sitelinks: int = 0

    # English aliases.
    aliases: Optional[List[str]] = None


class WikidataJsonDumpReader(DumpReader):
    """Reader for Wikidata JSON dump files.

    Parses lines into Entity objects.

    See https://www.wikidata.org/wiki/Wikidata:Database_download
    and https://doc.wikimedia.org/Wikibase/master/php/md_docs_topics_json.html.
    """
    def __init__(self,
        pattern: str,
        require_title=False,
        **kwargs):
        DumpReader.__init__(self,
            pattern=pattern,
            read_type=str,
            **kwargs)

        self.require_title = require_title

    def parse_line(self, line: str) -> Iterator[Entity]:
        # The JSON dumps look like
        # [
        # {...},
        # {...},
        # ...
        # ]
        line = line.strip()
        if not line:
            return
        if line.endswith(','):
            line = line[:-1]
        if line in ('[', ']'):
            return

        obj = json.loads(line)
        if 'id' not in obj:
            return

        normalized_title = obj.get('sitelinks', {}).get('enwiki', {}).get('title')
        if normalized_title:
            normalized_title = normalized_title.replace(' ', '_')

        e = Entity(
            qid=obj['id'],
            sitelinks=len(obj.get('sitelinks', {})),
            title=normalized_title,
            label=obj.get('labels', {}).get('en', {}).get('value'),
            aliases=[d['value'] for d in obj.get('aliases', {}).get('en', [])],
        )

        if self.require_title and not e.title:
            return
        yield e


class QRankEntry(NamedTuple):
    qid: str
    qrank: int


class QRankDumpReader(DumpReader):
    """Reader for QRank files.

    Parses lines into QRankEntry objects.

    See https://qrank.wmcloud.org/.
    """
    def __init__(self,
        pattern: str,
        expected_header=_header_path('qrank.csv.template'),
        **kwargs):
        DumpReader.__init__(self,
            pattern=pattern,
            read_type=str,
            expected_header=expected_header,
            **kwargs)

    def parse_line(self, line: str) -> Iterator[QRankEntry]:
        line = line.strip()
        if not line:
            return
        qid, _qrank = line.split(',')
        if qid == 'Entity':
            # Skip header line.
            return

        yield QRankEntry(
            qid=qid,
            qrank=int(_qrank),
        )


class Wikipedia2VecEntry(NamedTuple):
    name: str

    # Each entry is either for an entity or a word.
    is_entity: bool

    vector: List[float]


class Wikipedia2VecDumpReader(DumpReader):
    """Reader for Wikipedia2Vec embedding files.

    Parses lines into Wikipedia2VecEntry objects.

    Currently only the word2vec format is supported.

    See https://wikipedia2vec.github.io/wikipedia2vec/pretrained/
    and https://wikipedia2vec.github.io/wikipedia2vec/commands/#saving-embeddings-in-text-format
    """
    def __init__(self,
        pattern: str,
        format: str = 'word2vec',
        **kwargs):
        DumpReader.__init__(self,
            pattern=pattern,
            read_type=str,
            **kwargs)

        if format != 'word2vec':
            raise NotImplementedError(
                f'Requested format {format}, but only the word2vec format '
                'is supported.')

    def parse_line(self, line: str) -> Iterator[Wikipedia2VecEntry]:
        line = line.strip()
        if not line:
            return

        tokens = line.split(' ')
        if len(tokens) <= 2:
            # Skip the header line.
            return

        is_entity = False
        name = tokens[0]
        if name.startswith('ENTITY/'):
            is_entity = True
            name = name[7:]

        yield Wikipedia2VecEntry(
            name=name,
            is_entity=is_entity,
            vector=[float(t) for t in tokens[1:]],
        )
