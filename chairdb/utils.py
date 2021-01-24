import ijson

import contextlib
import json

from .db.datatypes import Document


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

def couchdb_json_to_doc(json):
    id = json.pop('_id')
    json.pop('_rev', None)  # for local docs
    if id.startswith('_local/'):
        id = id[len('_local/'):]
        rev_num, path = 0, None
    else:
        revs = json.pop('_revisions')
        rev_num, path = revs['start'], revs['ids']
    body = None if json.get('_deleted') else json
    return Document(id, rev_num, path, body)


def doc_to_couchdb_json(doc):
    if doc.is_local:
        json = {'_id': f'_local/{doc.id}'}
    else:
        revs = {'start': doc.rev_num, 'ids': doc.path}
        json = {'_id': doc.id, '_revisions': revs}
    if doc.deleted:
        json['_deleted'] = True
    else:
        json.update(doc.body)
    return json


def rev(rev_num, rev_hash):
    return f'{rev_num}-{rev_hash}'


def parse_rev(rev):
    num, hash = rev.split('-')
    return int(num), hash
