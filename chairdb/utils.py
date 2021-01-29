import ijson

import contextlib
import json

from .db.datatypes import Document, AbstractDocument


# JSON helpers
def as_json(item):
    return json.dumps(item, separators=(",", ":"))


async def parse_json_stream(stream, type, prefix):
    results = ijson.sendable_list()
    coro = getattr(ijson, type + '_coro')(results, prefix)
    async for chunk in stream:
        with contextlib.suppress(StopIteration):
            coro.send(chunk)
        for result in results:
            yield result
        results.clear()


async def json_array_inner(header, iterator, gen_footer):
    text = header
    async for i, item in aenumerate(iterator):
        if i > 0:
            yield text
            text = f',\n{item}'
        else:
            text = f'{text}{item}'
    yield f'{text}{gen_footer()}'


async def json_object_inner(header, iterator, gen_footer):
    generator_exp = (f'{key}:{value}' async for key, value in iterator)
    async for json_part in json_array_inner(header, generator_exp, gen_footer):
        # json array and a json object are both comma separated
        yield json_part


# async helpers
async def aenumerate(iterable):
    counter = 0
    async for item in iterable:
        yield counter, item
        counter += 1


async def async_iter(iterable):
    for item in iterable:
        yield item


async def to_list(asynciterable):
    return [x async for x in asynciterable]


async def peek(aiterable, n=2):
    first_n = await to_list(take_n(aiterable, n))
    return first_n, combine(first_n, aiterable)


async def take_n(aiterable, n):
    async for i, item in aenumerate(aiterable):
        yield item
        if i + 1 == n:
            break


async def combine(iterable, aiterable):
    for item in iterable:
        yield item
    async for item in aiterable:
        yield item


# couchdb helpers

class LocalDocument(AbstractDocument):
    pass


def couchdb_json_to_doc(json, id=None):
    """Returns a (local_id, document) tuple. local_id is only defined for local
    documents

    """
    id = json.pop('_id', id)
    # default to None for local docs:
    json.pop('_rev', None)
    revs = json.pop('_revisions', None)
    body = None if json.get('_deleted') else json
    if id.startswith('_local/'):
        id = id[len('_local/'):]
        doc = LocalDocument(id, body)
    else:
        rev_num, path = revs['start'], tuple(revs['ids'])
        doc = Document(id, rev_num, path, body)
    return doc


def doc_to_couchdb_json(doc):
    if isinstance(doc, LocalDocument):
        json = {'_id': f'_local/{doc.id}', '_rev': rev(0, '1')}
    else:
        r = rev(doc.rev_num, doc.path[0])
        revs = {'start': doc.rev_num, 'ids': doc.path}
        json = {'_id': doc.id, '_rev': r, '_revisions': revs}
    if doc.is_deleted:
        json['_deleted'] = True
    else:
        json.update(doc.body)
    return json


def rev(rev_num, rev_hash):
    return f'{rev_num}-{rev_hash}'


def parse_rev(rev):
    num, hash = rev.split('-')
    return int(num), hash
