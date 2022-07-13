"""Transforms for processing categories."""

import apache_beam as beam


def JoinPagesAndCategorylinks(pages, categorylinks):
    """Join data from pages and categorylinks, keeyd by page_title.

    Drops redirect pages.

    TODO: don't process redirects here.

    Returns: PCollection of tuples:
        (page_title, [category name]) 

    Args:
        pages: PCollection of (int page_id, str page_title,
        bool is_redirect).
        
        categorylinks: PCollection of (int page_id, str category
        name)
    """

    def drop_redirects(page_id, page_title, is_redirect):
        if not is_redirect:
            yield (page_id, page_title)

    pages_filtered = pages | 'DropRedirects' >> beam.FlatMapTuple(drop_redirects)

    join_sources = {
        'titles': pages_filtered,
        'cats': categorylinks
    }

    # `join_item` should look like (page_id, {'titles': [...], 'cats': [...]}).
    def process_join(join_item):
        page_id, d = join_item
        if not d.get('titles') or not d.get('cats'):
            return

        # Assume titles are unique.
        # TODO: verify this.
        yield (
            d['titles'][0],  
            d['cats'],
        )

    return (join_sources 
            | 'JoinPagesAndCategorylinks' >> beam.CoGroupByKey() 
            | 'ProcessJoinPagesAndCategorylinks' >> beam.FlatMap(process_join))
