import contextlib
import json

import anyio
import httpx

from .datatypes import Unauthorized, Forbidden, NotFound, Change, Missing
from ..multipart import MultipartStreamParser
from ..utils import (as_json, couchdb_json_to_doc, doc_to_couchdb_json, anext,
                     json_object_inner, add_http_attachments, parse_rev, rev,
                     verify_no_attachments, parse_json_stream)


JSON_REQ_HEADERS = {'Content-Type': 'application/json'}
MAX_CONNECTIONS = 10


class HTTPDatabase(httpx.AsyncClient):
    """Allows accessing remote databases with a CouchDB-compatible HTTP API
    through a high-level API similar to that of InMemoryDatabase. This makes it
    possible to replicate to and from such databases. Ideally, as much of the
    (messy) HTTP reality is hidden from the user as possible. This is done e.g.
    by minimizing memory usage through streaming requests and responses.

    The additional 'create' method is used as part of the replication protocol,
    but InMemoryDatabase-s don't need it as they always exist.

    """
    def __init__(self, url, credentials=None, *args, **kwargs):
        limits = httpx.Limits(max_keepalive_connections=MAX_CONNECTIONS // 2,
                              max_connections=MAX_CONNECTIONS)
        super().__init__(base_url=url, limits=limits, *args, **kwargs)

        self._credentials = None
        if credentials:
            name, password = credentials
            self._credentials = {'name': name, 'password': password}

    async def create(self):
        """True if database creation succeeded, False otherwise."""

        resp = await self._request('PUT', '/')
        if resp.status_code == httpx.codes.PRECONDITION_FAILED:
            return False  # already exists
        assert resp.status_code == httpx.codes.CREATED
        return True

    async def destroy(self):
        """Handy, e.g. for testing, but not actually used by during replication

        """
        return (await self._request('DELETE', '/')).json()

    @property
    def update_seq(self):
        return self._get_update_seq()

    async def _get_update_seq(self):
        resp = await self._request('GET', '/')
        if resp.status_code == httpx.codes.NOT_FOUND:
            raise NotFound()
        else:
            assert resp.status_code == httpx.codes.OK
            return resp.json()['update_seq']

    @property
    def id(self):
        return self._get_id()

    async def _get_id(self):
        base_id = (await self._request('GET', '../')).json()['uuid']
        return base_id + str(self.base_url) + 'remote'

    async def changes(self, since=None, continuous=False):
        params = {'style': 'all_docs'}
        opts = {}
        if since is not None:
            params['since'] = since
        if continuous:
            params['feed'] = 'continuous'
            opts['timeout'] = None
        stream = self._stream('GET', '/_changes', params=params, **opts)
        async with stream as resp:
            async for c in self._parse_changes_response(resp, continuous):
                deleted = c.get('deleted', False)
                leaf_revs = [parse_rev(item['rev']) for item in c['changes']]
                yield Change(c['id'], c['seq'], deleted, leaf_revs)

    async def _parse_changes_response(self, resp, continuous):
        assert resp.status_code == httpx.codes.OK
        if continuous:
            async for line in resp.aiter_lines():
                if line.strip():
                    yield json.loads(line)
        else:
            async for obj in parse_json_stream(resp.aiter_bytes(), 'items',
                                               'results.item'):
                yield obj

    async def revs_diff(self, remote):
        items = self._revs_diff_json(remote)
        body = self._encode_aiter(json_object_inner('{', items, lambda: '}\n'))
        async with self._stream('POST', '/_revs_diff', data=body,
                                headers=JSON_REQ_HEADERS) as resp:
            assert resp.status_code == httpx.codes.OK

            async for id, info in parse_json_stream(resp.aiter_bytes(),
                                                    'kvitems', ''):
                missing_revs = [parse_rev(r) for r in info['missing']]
                pa = [parse_rev(r) for r in info.get('possible_ancestors', [])]
                yield Missing(id, missing_revs, pa)

    async def _revs_diff_json(self, remote):
        async for id, revs in remote:
            yield as_json(id), as_json([rev(*r) for r in revs])

    async def _encode_aiter(self, aiterable):
        async for part in aiterable:
            yield part.encode('UTF-8')

    async def write(self, doc):
        params = {'new_edits': False}
        doc_json = await doc_to_couchdb_json(doc)
        await self._request('PUT', f'/{doc.id}', params=params, json=doc_json)

    async def write_local(self, id, doc):
        await self._request('PUT', f'/_local/{id}', json=doc)

    async def read(self, id, **opts):
        verify_no_attachments(opts)

        async with self._start_read(id, **opts) as args:
            # no task group required without attachments
            async for doc in self._read(*args, tg=None):
                yield doc

    @contextlib.asynccontextmanager
    async def _start_read(self, id, revs=None, att_names=None,
                          atts_since=None):
        assert att_names is None
        params = {'latest': 'true', 'revs': 'true'}
        if revs == 'all':
            params['open_revs'] = 'all'
        elif revs is not None:
            params['open_revs'] = as_json([rev(*r) for r in revs])
        if atts_since is not None:
            params['atts_since'] = as_json([rev(*r) for r in atts_since])

        async with self._stream('GET', '/' + id, params=params) as resp:
            yield params, resp

    async def _read(self, params, resp, tg):
        if resp.status_code == httpx.codes.NOT_FOUND:
            message = json.loads(await resp.aread())
            yield NotFound(message)
        else:
            assert resp.status_code == httpx.codes.OK

            if 'open_revs' in params:
                # multiple docs
                assert resp.status_code == httpx.codes.OK
                async for part in MultipartStreamParser(resp):
                    yield await self._read_single_doc(part, tg)
            else:
                yield await self._read_single_doc(resp, tg)

    async def _read_single_doc(self, resp, tg):
        if resp.headers['Content-Type'].startswith('multipart/'):
            parser = MultipartStreamParser(resp).__aiter__()
            part = await anext(parser)
            assert part.headers == {'Content-Type': 'application/json'}
            doc, todo = couchdb_json_to_doc(json.loads(await part.aread()))
            add_http_attachments(doc, todo, parser, tg)
            return doc
        else:
            assert resp.headers['Content-Type'] == 'application/json'
            doc, todo = couchdb_json_to_doc(json.loads(await resp.aread()))
            assert not todo
            return doc

    @contextlib.asynccontextmanager
    async def read_with_attachments(self, id, **opts):
        send_stream, receive_stream = anyio.create_memory_object_stream()
        async with self._start_read(id, **opts) as args:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._read_to_stream, send_stream, *args, tg)
                async with receive_stream:
                    yield receive_stream

    async def _read_to_stream(self, send_stream, *args):
        async with send_stream:
            async for doc in self._read(*args):
                await send_stream.send(doc)

    async def read_local(self, id):
        doc = await anext(self.read('_local/' + id))
        with contextlib.suppress(AttributeError):
            return doc.body

    async def ensure_full_commit(self):
        await self._request('POST', '_ensure_full_commit')

    # helpers
    async def _request(self, *args, **kwargs):
        await self._handle_log_in()
        resp = await self.request(*args, **kwargs)
        return self._checked_resp(resp)

    async def _handle_log_in(self):
        if self._credentials:
            await self.post('../_session', json=self._credentials)
            self._credentials = None

    def _checked_resp(self, resp):
        if resp.status_code == httpx.codes.UNAUTHORIZED:
            raise Unauthorized(resp.json())
        if resp.status_code == httpx.codes.FORBIDDEN:
            raise Forbidden(resp.json())
        return resp

    @contextlib.asynccontextmanager
    async def _stream(self, *args, **kwargs):
        await self._handle_log_in()
        async with self.stream(*args, **kwargs) as resp:
            try:
                yield self._checked_resp(resp)
            except httpx.ResponseNotRead:  # retry after reading
                await resp.aread()
                yield self._checked_resp(resp)
