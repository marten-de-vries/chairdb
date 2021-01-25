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

import uuid

from ..utils import (async_iter, to_list, aenumerate, peek, as_json,
                     parse_json_stream, rev, couchdb_json_to_doc,
                     doc_to_couchdb_json, parse_rev)
from ..db.datatypes import LocalDocument, NotFound
from ..db.shared import rev_tuple
from .utils import JSONResp, parse_query_arg

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


class Database(HTTPEndpoint):
    async def get(self, request):
        return JSONResp({
            "instance_start_time": "0",
            "update_seq": await get_db(request).update_seq
        })

    def post(self, request):
        raise NotImplementedError()


def changes(request):
    if parse_query_arg(request, 'style', default='main_only') != 'all_docs':
        print('style =/= all_docs, but we do that anyway!')
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


async def stream_changes(changes):
    yield '{"results": [\n'
    last_seq = 0

    async for i, change in aenumerate(changes):
        if i > 0:
            yield ',\n'
        yield change_row_json(change)
        last_seq = change.seq
    yield f'\n], "last_seq": {last_seq}, "pending": 0}}\n'


async def revs_diff(request):
    remote = parse_json_stream(request.stream(), 'kvitems', '')
    generator = stream_revs_diff(get_db(request), parse_revs(remote))
    return StreamingResponse(generator, media_type='application/json')


async def parse_revs(data):
    async for id, revs in data:
        yield id, [parse_rev(r) for r in revs]


async def stream_revs_diff(db, remote):
    yield '{'
    result = db.revs_diff(remote)
    async for i, missing in aenumerate(result):
        if i > 0:
            yield ','
        revs = [rev(*r) for r in missing.missing_revs]
        yield f'{as_json(missing.id)}:{as_json({"missing": revs})}'
    yield '}\n'


async def ensure_full_commit(request):
    await get_db(request).ensure_full_commit()
    return JSONResp(FULL_COMMIT, 201)


def all_docs(request):
    generator = all_docs_stream(get_db(request))
    return StreamingResponse(generator, media_type='application/json')


def all_docs_stream(db):
    yield '{"offset":0,"rows":['
    total = 0
    for key, doc_info in db._byid.items():
        branch = doc_info.rev_tree[doc_info.winning_branch_idx]
        if branch.leaf_doc_ptr:
            if total != 0:
                yield ','
            r = rev(*rev_tuple(branch, branch.leaf_rev_num))
            yield as_json({'id': key, 'key': key, 'value': {'rev': r}})
            total += 1
    yield f'],"total_rows":{total}}}\n'


async def bulk_docs(request):
    req = await request.json()
    assert not req.get('new_edits', True)
    docs = [couchdb_json_to_doc(json) for json in req['docs']]
    generator = bulk_docs_stream(get_db(request), docs)
    return StreamingResponse(generator, 201, media_type='application/json')


async def bulk_docs_stream(db, req):
    yield '['
    result = db.write(async_iter(req))
    async for i, error in aenumerate(result):
        if i > 0:
            yield ','
        yield as_json(error)  # crash likely
    yield ']\n'


def local_docs(request):
    generator = stream_local_docs(get_db(request))
    return StreamingResponse(generator, media_type='application/json')


def stream_local_docs(db):
    yield '{"rows": ['
    for i, id in enumerate(db._local.keys()):
        if i > 0:
            yield ','
        full_id = f"_local/{id}"
        yield as_json({
            'id': full_id,
            'key': full_id,
            'value': {'rev': '0-1'},
        })
    yield ']}\n'


async def bulk_get(request):
    if not parse_query_arg(request, 'revs', default=False):
        print('revs=true not requested, but we do it anyway!')
    req = parse_bulk_get_request(request)
    generator = stream_bulk_get(get_db(request), req)
    return StreamingResponse(generator, media_type='application/json')


async def parse_bulk_get_request(request):
    async for id, docs in group_by(request.stream(), key=lambda d: d['id']):
        try:
            revs = [parse_rev(doc['rev']) for doc in docs]
        except KeyError:
            if id.startswith('_local/'):
                id = id[len('_local/'):]
                revs = 'local'
            else:
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


async def stream_bulk_get(db, req):
    yield '{"results": ['
    async for i, doc in aenumerate(db.read(req)):
        json = doc_to_couchdb_json(doc)
        yield as_json({'docs': [{'ok': json}], 'id': doc.id})
    yield ']}\n'


class DocumentEndpoint(HTTPEndpoint):
    async def get(self, request):
        if not parse_query_arg(request, 'revs', default=False):
            print('revs=true not requested, but we do it anyway!')

        doc_id = request.path_params['id']
        if doc_id.startswith('_local/'):
            doc_id = doc_id[len('_local/'):]
            revs = 'local'
        else:
            revs = self._parse_revs(request)

        resp = get_db(request).read(async_iter([(doc_id, revs)]))

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
            return JSONResp(first_two[0])

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
                print('latest=true not requested, but we do it anyway!')
            revs = [parse_rev(r) for r in revs]
        return revs

    async def _multipart_response(self, items, boundary):
        async for item in items:
            yield f'--{boundary}\r\nContent-Type: application/json\r\n\r\n'
            yield f'{as_json(doc_to_couchdb_json(item))}\r\n'
        yield f'--{boundary}--'

    async def put(self, request):
        doc_id = request.path_params['id']
        if not doc_id.startswith('_local/'):
            return JSONResp(DOC_NOT_FOUND, 404)  # FIXME

        doc = couchdb_json_to_doc(await request.json())

        await to_list(get_db(request).write(async_iter([doc])))
        return JSONResp({'id': doc_id, 'rev': '0-1'}, 201)

    async def delete(self, request):
        doc_id = request.path_params['id']
        assert doc_id.startswith('_local/')

        docs = async_iter([LocalDocument(doc_id, body=None)])
        try:
            await get_db(request).write(docs).__anext__()
        except StopAsyncIteration:
            return JSONResp({'id': doc_id, 'ok': True, 'rev': '0-1'})
        else:
            return JSONResp(DOC_NOT_FOUND, 404)

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
