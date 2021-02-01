"""A CouchDB-compatible HTTP server backed by a database specified by
app.state.db or request.state.db. One of these can either be set manually
(when only a limited amount of databases needs to be exposed), or
programmatically by middleware. An example of that is given by __init__, which
builds a full CouchDB-compatible server out of in-memory databases.

"""

from starlette.applications import Starlette
from starlette.endpoints import HTTPEndpoint
from starlette.responses import StreamingResponse
from starlette.routing import Route

import asyncio
import contextlib
import functools
import json
import logging
import uuid

from ..utils import (async_iter, peek, as_json, json_object_inner,
                     parse_json_stream, rev, couchdb_json_to_doc, parse_rev,
                     doc_to_couchdb_json, json_array_inner, LocalDocument,
                     combine, to_list)
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

    def post(self, request):
        raise NotImplementedError()


# changes
def changes(request):
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
    row = {'id': id, 'seq': seq, 'deleted': deleted, 'changes': changes}
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
def revs_diff(request):
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


def all_docs(request):
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
        if isinstance(doc, LocalDocument):
            await db.write_local(doc.id, doc.body)
        else:
            writes.append(doc)
    async for result in db.write(async_iter(writes)):
        yield as_json(result)


# local docs
def local_docs(request):
    docs_json = local_docs_json(get_db(request))
    generator = json_array_inner('{"rows": [', docs_json, lambda: ']}\n')
    return StreamingResponse(generator, media_type='application/json')


async def local_docs_json(db):
    for id in db._local.keys():
        full_id = f"_local/{id}"
        yield as_json({
            'id': full_id,
            'key': full_id,
            'value': {'rev': '0-1'},
        })


# bulk get
def bulk_get(request):
    if not parse_query_arg(request, 'revs', default=False):
        logger.warn('revs=true not requested, but we do it anyway!')
    local_docs = []
    req = parse_bulk_get_request(request, local_docs)
    results = bulk_get_json(get_db(request), req, local_docs)

    generator = json_array_inner('{"results": [', results, lambda: ']}\n')
    return StreamingResponse(generator, media_type='application/json')


async def parse_bulk_get_request(request, local_docs):
    async for id, docs in group_by(request.stream(), key=lambda d: d['id']):
        if id.startswith('_local/'):
            local_docs.append(id[len('_local/'):])
        else:
            try:
                revs = [parse_rev(doc['rev']) for doc in docs]
            except KeyError:
                revs = 'all'
            yield id, revs


async def group_by(stream, key):
    try:
        first_doc = await stream.__anext__()
    except StopAsyncIteration:
        return

    last_key, docs = key(first_doc), [first_doc]
    for doc in stream:
        if key(doc) != last_key:
            yield last_key, docs
            last_key, docs = key(doc), []
        docs.append(stream)
    yield last_key, docs


async def bulk_get_json(db, req, local_docs):
    local_reads = (db.read_local(doc) for doc in local_docs)
    local_results = await asyncio.gather(local_reads)
    # TODO: something more gradual maybe?
    async for doc in combine(local_results, db.read(req)):
        json = await doc_to_couchdb_json(doc)
        yield as_json({'docs': [{'ok': json}], 'id': doc.id})


# /doc
class DocumentEndpoint(HTTPEndpoint):
    async def get(self, request):
        db = get_db(request)
        doc_id = request.path_params['id']
        if doc_id.startswith('_local/'):
            local_id = doc_id[len('_local/'):]
            doc = await db.read_local(local_id)
            # TODO: cleanup
            if not doc:
                resp = async_iter([NotFound()])
            else:
                resp = async_iter([LocalDocument(local_id, doc)])
        else:
            revs = self._parse_revs(request)
            # TODO: add more args
            resp = db.read(async_iter([(doc_id, revs)]))
            if not parse_query_arg(request, 'revs', default=False):
                logger.warn('revs=true not requested, but we do it anyway!')

        first_two, resp_orig = await peek(resp, n=2)
        if isinstance(first_two[0], NotFound):
            return JSONResp(DOC_NOT_FOUND, 404)

        multi = len(first_two) == 2
        if multi or 'application/json' not in request.headers['accept']:
            # multipart
            boundary = uuid.uuid4().hex
            mt = f'multipart/mixed; boundary="{boundary}"'
            generator = self._multipart_response(resp_orig, boundary)
            return StreamingResponse(generator, media_type=mt)
        else:
            return JSONResp(await doc_to_couchdb_json(first_two[0]))

    def _parse_revs(self, request):
        rev = parse_query_arg(request, 'rev')
        revs = parse_query_arg(request, 'open_revs')
        if revs is None:
            if rev is None:
                revs = 'winner'
            else:
                revs = [rev]
        if revs not in ['winner', 'all']:
            if not parse_query_arg(request, 'latest'):
                logger.warn('latest=true not requested, but we do it anyway!')
            revs = [parse_rev(r) for r in revs]
        return revs

    async def _multipart_response(self, items, boundary):
        async for item in items:
            yield f'--{boundary}\r\nContent-Type: application/json\r\n\r\n'
            yield f'{as_json(await doc_to_couchdb_json(item))}\r\n'
        yield f'--{boundary}--'

    async def put(self, request):
        doc_id = request.path_params['id']
        if request.headers['Content-Type'] == 'application/json':
            doc, todo = couchdb_json_to_doc(await request.json(), doc_id)
            assert not todo
        else:
            parser = MultipartStreamParser(request).__aiter__()
            first = await parser.__anext__()
            assert first.headers == {'Content-Type': 'application/json'}
            doc_json = json.loads(await first.aread())
            doc, todo = couchdb_json_to_doc(doc_json)
            # TODO: add attachments in 'todo' to the doc & read them

        if isinstance(doc, LocalDocument):
            await get_db(request).write_local(doc.id, doc.body)
        else:
            assert not parse_query_arg(request, 'new_edits', default=True)
            await to_list(get_db(request).write(async_iter([doc])))

        return JSONResp({'id': doc_id, 'rev': '0-1'}, 201)

    async def delete(self, request):
        doc_id = request.path_params['id']
        assert doc_id.startswith('_local/')

        await get_db(request).write_local(doc_id[len('_local/'):], None)
        return JSONResp({'id': doc_id, 'ok': True, 'rev': '0-1'})

    def copy(sef, request):
        raise NotImplementedError()


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
        Route('/_local_docs', local_docs),
        Route('/_bulk_get', bulk_get, methods=['POST']),
        Route('/{id:path}', DocumentEndpoint),
    ], **opts)
