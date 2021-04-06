import anyio
import ijson

import base64
import contextlib
import hashlib
import json
import typing
import zlib

from .db.datatypes import (Document, AbstractDocument, AttachmentStub,
                           AttachmentMetadata)


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
async def anext(docs):
    return await docs.__anext__()


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


# couchdb helpers
def verify_no_attachments(opts):
    if 'att_names' in opts or 'atts_since' in opts:
        raise ValueError('cannot retrieve attachments')


class LocalDocument(AbstractDocument):
    pass


def couchdb_json_to_doc(json, id=None):
    """Returns a doc, todo tuple. The first is either a LocalDocument or a
    Document, while the second contains follow: true attachments that still
    have to be added to doc.attachments to complete the conversion.

    """
    id = json.pop('_id', id)
    # default to None for local docs:
    try:
        rev_num, rev_hash = parse_rev(json.pop('_rev'))
    except KeyError:
        revs_default = None
    else:
        revs_default = {'start': rev_num, 'ids': [rev_hash]}
    revs = json.pop('_revisions', revs_default)
    body = None if json.get('_deleted') else json
    todo = []
    if id.startswith('_local/'):
        id = id[len('_local/'):]
        doc = LocalDocument(id, body)
    else:
        atts = None if body is None else parse_attachments(body, todo)
        rev_num, path = revs['start'], tuple(revs['ids'])
        doc = Document(id, rev_num, path, body, atts)
    return doc, todo


def parse_attachments(body, todo):
    atts = {}
    for name, info in body.pop('_attachments', {}).items():
        meta = AttachmentMetadata(info['revpos'], info['content_type'],
                                  info['length'], info['digest'])
        if info.pop('stub', False):
            atts[name] = AttachmentStub(meta)
        else:
            try:
                data = base64.b64decode(info['data'])
            except KeyError:
                assert info['follows']
                todo.append((name, meta))
            else:
                atts[name] = InMemoryAttachment(meta, data)
    return atts


class InMemoryAttachment(typing.NamedTuple):
    meta: AttachmentMetadata
    data: bytes
    is_stub: bool = False

    def __iter__(self):
        yield self.data  # sync API

    async def __aiter__(self):
        yield self.data  # async API


async def doc_to_couchdb_json(doc):
    try:
        r = rev(doc.rev_num, doc.path[0])
    except IndexError:
        r = None  # for docs that were just created and are new edits
    revs = {'start': doc.rev_num, 'ids': doc.path}
    json = {'_id': doc.id, '_rev': r, '_revisions': revs}
    if doc.attachments:
        json['_attachments'] = await generate_attachments_json(doc)

    if doc.is_deleted:
        json['_deleted'] = True
    else:
        json.update(doc.body)
    return json


async def generate_attachments_json(doc):
    atts = {}
    for key, att in doc.attachments.items():
        atts[key] = {
            'content_type': att.meta.content_type,
            'digest': att.meta.digest,
            'length': att.meta.length,
            'revpos': att.meta.rev_pos,
        }
        if att.is_stub:
            atts[key]['stub'] = True
        else:
            data = bytearray()
            async for chunk in att:
                data.extend(chunk)
            atts[key]['data'] = base64.b64encode(data).decode('ascii')
    return atts


def add_http_attachments(doc, todo, parser, tg):
    send_streams = {}
    for name, meta in todo:
        send_streams[name], rec_stream = anyio.create_memory_object_stream()
        doc.attachments[name] = HTTPAttachment(meta, rec_stream)
    tg.start_soon(parse_atts, send_streams, parser)


class HTTPAttachment(typing.NamedTuple):
    meta: AttachmentMetadata
    receive_stream: anyio.streams.memory.MemoryObjectReceiveStream
    is_stub: bool = False

    async def __aiter__(self):
        async with self.receive_stream:
            async for chunk in self.receive_stream:
                yield chunk


async def parse_atts(send_streams, parser):
    async for attachment in parser:
        _, name, _ = attachment.headers['Content-Disposition'].split('"')

        aiterator = attachment.aiter_bytes()
        if attachment.headers.get('Content-Encoding') == 'gzip':
            aiterator = _unzip(aiterator)

        stream = send_streams[name]
        async with stream:
            async for chunk in aiterator:
                if chunk:
                    await stream.send(chunk)


async def _unzip(chunks):
    decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
    async for chunk in chunks:
        yield decompressor.decompress(chunk)
    yield decompressor.flush()


def rev(rev_num, rev_hash):
    return f'{rev_num}-{rev_hash}'


def parse_rev(rev):
    num, hash = rev.split('-')
    return int(num), hash


async def new_edit(updated_doc):
    # TODO: only include attachment stubs? That would also make it possible to
    # make this function sync.
    hash = hashlib.md5()
    serialized = json.dumps(await doc_to_couchdb_json(updated_doc))
    hash.update(serialized.encode('UTF-8'))

    updated_doc.rev_num += 1
    updated_doc.path = (hash.hexdigest(),) + updated_doc.path

    return updated_doc


# misc

class hashabledict(dict):
    def __hash__(self):
        return hash(tuple(self.items()))
