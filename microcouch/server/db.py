from starlette.applications import Starlette
from starlette.endpoints import HTTPEndpoint
from starlette.responses import StreamingResponse
from starlette.routing import Route

import aioitertools

import uuid

from ..utils import async_iter, to_list
from .utils import JSONResp, parse_query_arg, as_json

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
    style = parse_query_arg(request, 'style', default='main_only')
    assert style == 'all_docs'

    generator = stream_changes(get_db(request))
    return StreamingResponse(generator, media_type='application/json')


async def stream_changes(db):
    yield '{"results": ['
    last_seq = 0

    async for i, change in aioitertools.enumerate(db.changes()):
        if i > 0:
            yield ','
        id, seq, deleted, leaf_revs = change
        changes = [{'_rev': rev} for rev in leaf_revs]
        row = {'id': id, 'seq': seq, 'deleted': deleted, 'changes': changes}
        yield as_json(row)
        last_seq = change.seq
    yield f'], "last_seq": {last_seq}, "pending": 0}}\n'


async def revs_diff(request):
    remote = await request.json()
    generator = stream_revs_diff(get_db(request), remote.items())
    return StreamingResponse(generator, media_type='application/json')


async def stream_revs_diff(db, remote):
    yield '{'
    result = db.revs_diff(async_iter(remote))
    async for i, (id, info) in aioitertools.enumerate(result):
        if i > 0:
            yield ','
        yield f'{as_json(id)}:{as_json(info)}'
    yield '}\n'


def ensure_full_commit(request):
    return JSONResp(FULL_COMMIT, 201)


def all_docs(request):
    generator = all_docs_stream(get_db(request))
    return StreamingResponse(generator, media_type='application/json')


def all_docs_stream(db):
    yield '{"offset":0,"rows":['
    total = 0
    for key, doc_info in db._byid.items():
        if doc_info.winner_leaf.doc_ptr:
            if total != 0:
                yield ','
            rev = f'{doc_info.winner_rev_num}-{doc_info.winner_leaf.rev_hash}'
            yield as_json({'id': key, 'key': key, 'value': {'rev': rev}})
            total += 1
    yield f'],"total_rows":{total}}}\n'


async def bulk_docs(request):
    req = await request.json()
    assert not req.get('new_edits', True)
    generator = bulk_docs_stream(get_db(request), req['docs'])
    return StreamingResponse(generator, 201, media_type='application/json')


async def bulk_docs_stream(db, req):
    yield '['
    result = db.write(async_iter(req))
    async for i, error in aioitertools.enumerate(result):
        if i > 0:
            yield ','
        yield as_json(error)  # crash likely
    yield ']\n'


def local_docs(request):
    generator = stream_local_docs(get_db(request))
    return StreamingResponse(generator, media_type='application/json')


def stream_local_docs(request):
    yield '{"rows": ['
    for i, doc in enumerate(db.local.values()):
        if i > 0:
            yield ','
        yield as_json({
            'id': doc['_id'],
            'key': doc['_id'],
            'value': {'rev': '0-1'},
        })
    yield ']}\n'


async def bulk_get(request):
    req = {}
    for doc in (await request.json())['docs']:
        revs = req.setdefault(doc['_id'], [])
        try:
            revs.append(doc['_rev'])
            assert parse_query_arg(request, 'latest')
        except KeyError:
            req[doc['_id']] = 'all'
        include_path = parse_query_arg(request, 'revs')
    generator = stream_bulk_get(get_db(request), req, include_path)
    return StreamingResponse(generator, media_type='application/json')


async def stream_bulk_get(db, req, include_path):
    yield '{"results": ['
    result = db.read(req.items(), include_path)
    async for i, doc in aioitertools.enumerate(result):
        yield as_json({'docs': [{'ok': doc}], 'id': doc['_id']})
    yield ']}\n'


class Document(HTTPEndpoint):
    # local docs
    async def get(self, request):
        doc_id = request.path_params['id']
        if doc_id.startswith('_local/'):
            return self._get_local(get_db(request), doc_id[len('_local/'):])
        else:
            return await self._get_normal(request, doc_id)

    def _get_local(self, db, doc_id):
        try:
            return JSONResp(db.local[doc_id])
        except KeyError:
            return JSONResp(DOC_NOT_FOUND, 404)

    async def _get_normal(self, request, doc_id):
        rev = parse_query_arg(request, 'rev')
        revs = parse_query_arg(request, 'open_revs')
        if revs is None:
            if rev is None:
                revs = 'winner'
            else:
                revs = [rev]
        assert revs in ['winner', 'all'] or parse_query_arg(request, 'latest')
        include_path = parse_query_arg(request, 'revs', default=False)
        resp = get_db(request).read(async_iter([(doc_id, revs)]), include_path)

        resp_peek, resp_orig = aioitertools.tee(resp)
        try:
            first_two = await to_list(aioitertools.islice(resp_peek, 2))
        except KeyError:
            return JSONResp(DOC_NOT_FOUND, 404)
        else:
            multi = len(first_two) == 2
        if multi or 'application/json' not in request.headers['accept']:
            # multipart
            boundary = uuid.uuid4().hex
            mt = f'multipart/mixed; boundary={boundary}'
            generator = self._multipart_response(resp_orig, boundary)
            return StreamingResponse(generator, media_type=mt)
        else:
            return JSONResp(aioitertools.next(resp_orig))

    async def _multipart_response(self, items, boundary):
        async for item in items:
            yield f'--{boundary}\r\nContent-Type: application/json\r\n\r\n'
            yield f'{as_json(item)}\r\n'
        yield f'--{boundary}--'

    async def put(self, request):
        doc_id = request.path_params['id']
        if not doc_id.startswith('_local/'):
            return JSONResp(DOC_NOT_FOUND, 404)  # FIXME
        doc_id = doc_id[len('_local/'):]

        doc = await request.json()
        get_db(request).local[doc_id] = doc
        return JSONResp({'id': doc_id, 'rev': '0-1'}, 201)

    def delete(self, request):
        doc_id = request.path_params['id']
        assert doc_id.startswith('_local/')
        doc_id = doc_id[len('_local/'):]

        try:
            del get_db(request).local[doc_id]
        except KeyErrror:
            return JSONResp(DOC_NOT_FOUND, 404)
        return JSONResp({'id': doc_id, 'ok': True, 'rev': '0-1'})

    def copy(sef, request):
        raise NotImplementedError()


# main entry function

def build_db_app(**opts):
    return Starlette(routes=[
        Route('/', Database),
        Route('/_changes', changes),
        Route('/_revs_diff', revs_diff, methods=['POST']),
        Route('/_ensure_full_commit', ensure_full_commit, methods=['POST']),
        Route('/_all_docs', all_docs),
        Route('/_bulk_docs', bulk_docs, methods=['POST']),
        Route('/_local_docs', local_docs),
        Route('/_bulk_get', bulk_get, methods=['POST']),
        Route('/{id:path}', Document),
    ], **opts)
