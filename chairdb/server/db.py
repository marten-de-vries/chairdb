"""A CouchDB-compatible HTTP server backed by a database specified by
app.state.db or request.state.db. One of these can either be set manually
(when only a limited amount of databases needs to be exposed), or
programmatically by middleware. An example of that is given by __init__, which
builds a full CouchDB-compatible server out of in-memory databases.

"""

import anyio
from starlette.applications import Starlette
from starlette.endpoints import HTTPEndpoint
from starlette.responses import StreamingResponse
from starlette.routing import Route

import contextlib
import functools
import json
import logging
import uuid

from ..utils import (async_iter, as_json, json_object_inner, parse_json_stream,
                     rev, couchdb_json_to_doc, parse_rev, doc_to_couchdb_json,
                     json_array_inner, LocalDocument, to_list,
                     add_http_attachments)
from ..db.datatypes import NotFound
from ..multipart import MultipartStreamParser
from .utils import JSONResp, parse_query_arg

logger = logging.getLogger(__name__)

FULL_COMMIT = {
    'instance_start_time': "0",
    'ok': True
}

DOC_NOT_FOUND = {
    "error": "not_found",
    "reason": "missing",
}


def get_db(request):
    try:
        return request.state.db
    except AttributeError:
        return request.app.state.db


def db_name(request):
    try:
        return request.state.db_name
    except AttributeError:
        return request.app.state.db_name


class Database(HTTPEndpoint):
    async def get(self, request):
        result = {
            "instance_start_time": "0",
            "update_seq": await get_db(request).update_seq
        }
        with contextlib.suppress(AttributeError):
            result['db_name'] = db_name(request)
        return JSONResp(result)


# changes
async def changes(request):
    if parse_query_arg(request, 'style', default='main_only') != 'all_docs':
        logger.warn('style =/= all_docs, but we do that anyway!')
    since = int(parse_query_arg(request, 'since', default=0))
    feed = parse_query_arg(request, 'feed', default='normal')
    continuous = feed == 'continuous'

    changes = get_db(request).changes(since, continuous)
    if continuous:
        generator = stream_changes_continuous(changes)
        return StreamingResponse(generator, media_type='text/plain')
    else:
        generator = stream_changes(changes)
        return StreamingResponse(generator, media_type='application/json')


async def stream_changes_continuous(changes):
    async for change in changes:
        yield f'{change_row_json(change)}\n'


def change_row_json(change):
    id, seq, deleted, leaf_revs = change
    changes = [{'rev': rev(*lr)} for lr in leaf_revs]
    row = {'seq': seq, 'id': id, 'changes': changes}
    if deleted:
        row['deleted'] = True
    return as_json(row)


def stream_changes(changes):
    info = {'last_seq': 0}
    changes = json_changes_and_last_seq(changes, info)
    gen_changes_footer = functools.partial(changes_footer, info)
    return json_array_inner('{"results": [\n', changes, gen_changes_footer)


async def json_changes_and_last_seq(changes, store):
    async for change in changes:
        yield change_row_json(change)
        store['last_seq'] = change.seq


def changes_footer(info):
    return f'\n], "last_seq": {info["last_seq"]}, "pending": 0}}\n'


# revs diff
async def revs_diff(request):
    remote = parse_json_stream(request.stream(), 'kvitems', '')
    remote_parsed = parse_revs(remote)
    result = get_db(request).revs_diff(remote_parsed)
    gen = json_object_inner('{', revs_diff_json_items(result), lambda: '}\n')
    return StreamingResponse(gen, media_type='application/json')


async def parse_revs(data):
    async for id, revs in data:
        yield id, [parse_rev(r) for r in revs]


async def revs_diff_json_items(result):
    async for missing in result:
        revs = [rev(*r) for r in missing.missing_revs]
        pa = [rev(*r) for r in missing.possible_ancestors]
        value = {"missing": revs, "possible_ancestors": pa}
        yield as_json(missing.id), as_json(value)


# ensure full commit
async def ensure_full_commit(request):
    await get_db(request).ensure_full_commit()
    return JSONResp(FULL_COMMIT, 201)


async def all_docs(request):
    info = {'total_rows': 0}
    items = all_docs_json(get_db(request), info)
    gen_footer = functools.partial(all_docs_footer, info)
    generator = json_array_inner('{"offset":0,"rows":[', items, gen_footer)

    return StreamingResponse(generator, media_type='application/json')


async def all_docs_json(db, store):
    for key, (rev_tree, _) in db._byid.items():
        branch = rev_tree.winner()
        if branch.leaf_doc_ptr:
            r = rev(*branch.leaf_rev_tuple)
            yield as_json({'id': key, 'key': key, 'value': {'rev': r}})
            store['total_rows'] += 1


def all_docs_footer(info):
    return f'],"total_rows":{info["total_rows"]}}}\n'


# bulk docs
async def bulk_docs(request):
    req = await request.json()
    assert not req.get('new_edits', True)

    json_results = write_all(get_db(request), req['docs'])
    generator = json_array_inner('[', json_results, lambda: ']\n')
    return StreamingResponse(generator, 201, media_type='application/json')


async def write_all(db, docs):
    writes = []
    for json_doc in docs:
        doc, todo = couchdb_json_to_doc(json_doc)
        assert not todo
        assert not isinstance(doc, LocalDocument)
        writes.append(doc)
    async for result in db.write(async_iter(writes)):
        yield as_json(result)


# /doc
class DocumentEndpoint(HTTPEndpoint):
    def doc_id(self, request):
        """Overridden by subclasses"""

        return request.path_params['id']

    async def get(self, request):
        db = get_db(request)
        doc_id = self.doc_id(request)
        revs, multi = self._parse_revs(request)
        atts_since = parse_query_arg(request, 'atts_since', [])
        async with db.read(doc_id, revs=revs, atts_since=atts_since) as resp:
            if not parse_query_arg(request, 'revs', default=False):
                logger.warn('revs=true not requested, but we do it anyway!')

            if multi:
                return await self._multi_response(resp)
            else:
                return await self._single_response(await resp.__anext__())

    def _parse_revs(self, request):
        rev = parse_query_arg(request, 'rev')
        revs = parse_query_arg(request, 'open_revs')
        # In the future, do whatever CouchDB decides to do:
        # https://github.com/apache/couchdb/issues/3362
        multi = revs is not None
        if revs is None and rev is not None:
            revs = [rev]
        if revs not in [None, 'all']:
            if not parse_query_arg(request, 'latest'):
                logger.warn('latest=true not requested, but we do it anyway!')
            revs = [parse_rev(r) for r in revs]
        return revs, multi

    async def _multi_response(self, docs):
        # multipart
        boundary = uuid.uuid4().hex
        mt = f'multipart/mixed; boundary="{boundary}"'
        generator = self._multipart_response(docs, boundary)
        return StreamingResponse(generator, media_type=mt)

    async def _multipart_response(self, items, boundary):
        async for item in items:
            yield f'--{boundary}\r\nContent-Type: application/json\r\n\r\n'
            yield f'{as_json(await doc_to_couchdb_json(item))}\r\n'
        yield f'--{boundary}--'

    async def _single_response(self, doc):
        if isinstance(doc, NotFound):
            return JSONResp(DOC_NOT_FOUND, 404)
        return JSONResp(await doc_to_couchdb_json(doc))

    async def put(self, request):
        doc_id = self.doc_id(request)
        async with anyio.create_task_group() as tg:
            if request.headers['Content-Type'] == 'application/json':
                doc, todo = couchdb_json_to_doc(await request.json(), doc_id)
                assert not todo
            else:
                parser = MultipartStreamParser(request).__aiter__()
                first = await parser.__anext__()
                assert first.headers == {'Content-Type': 'application/json'}
                doc_json = json.loads(await first.aread())
                doc, todo = couchdb_json_to_doc(doc_json, doc_id)
                add_http_attachments(doc, todo, parser, tg)

            if isinstance(doc, LocalDocument):
                await get_db(request).write_local(doc.id, doc.body)
            else:
                assert not parse_query_arg(request, 'new_edits', default=True)
                await to_list(get_db(request).write(async_iter([doc])))

        return JSONResp({'id': doc_id, 'rev': '0-1'}, 201)


class DesignDocumentEndpoint(DocumentEndpoint):
    def doc_id(self, request):
        return '_design/' + request.path_params['id']


class LocalDocumentEndpoint(DocumentEndpoint):
    def doc_id(self, request):
        return '_local/' + request.path_params['id']

    async def get(self, request):
        local_id = request.path_params['id']
        body = await get_db(request).read_local(local_id)
        if not body:
            return JSONResp(DOC_NOT_FOUND, 404)
        json = {'_id': f'_local/{local_id}', '_rev': '0-1'}
        json.update(body)
        return JSONResp(json)


class AttachmentEndpoint(HTTPEndpoint):
    def info(self, request):
        return request.path_params['id'], request.path_params['attachment']

    async def get(self, request):
        id, attachment = self.info(request)

        async with get_db(request).read(id, att_names=[attachment]) as docs:
            doc = await docs.__anext__()

            att = doc.attachments[attachment]
            resp = StreamingResponse(att, headers={
                'Content-Type': att.meta.content_type,
                'Content-Length': str(att.meta.length),
                'ETag': att.meta.digest,
                'Cache-Control': 'must-revalidate',
            })
            return resp


class DesignAttachmentEndpoint(AttachmentEndpoint):
    def info(self, request):
        id, attachment = super().info(request)
        return '_design/' + id, attachment


def build_db_app(**opts):
    """Instead of just exporting an app, we allow you to create one yourself
    such that you can add middleware to set e.g. request.state.db

    This is the main entry point for this module.

    """
    return Starlette(routes=[
        Route('/', Database),
        Route('/_changes', changes),
        Route('/_revs_diff', revs_diff, methods=['POST']),
        Route('/_ensure_full_commit', ensure_full_commit, methods=['POST']),
        Route('/_all_docs', all_docs),
        Route('/_bulk_docs', bulk_docs, methods=['POST']),
        Route('/_design/{id}', DesignDocumentEndpoint),
        Route('/_design/{id}/{attachment:path}', DesignAttachmentEndpoint),
        Route('/_local/{id}', LocalDocumentEndpoint),
        Route('/{id}', DocumentEndpoint),
        Route('/{id}/{attachment:path}', AttachmentEndpoint),
    ], **opts)
