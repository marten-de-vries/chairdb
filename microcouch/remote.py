import contextlib

import asyncio
import httpx
import json

from .datatypes import Unauthorized, Forbidden, NotFound, Change
from .utils import as_json, parse_json_stream, aenumerate
from .multipart import MultipartResponseParser


JSON_REQ_HEADERS = {'Content-Type': 'application/json'}
MAX_PARALLEL_READS = 10


class HTTPDatabase(httpx.AsyncClient):
    """Allows accessing remote databases with a CouchDB-compatible HTTP API
    through a high-level API similar to that of InMemoryDatabase. This makes it
    possible to replicate to and from such databases. Ideally, as much of the
    (messy) HTTP reality is hidden from the user as possible. This is done e.g.
    by minimizing memory usage through streaming requests and responses.

    The additional 'create' method is used as part of the replication protocol,
    but InMemoryDatabase-s don't need it as they always exist.

    """
    # TODO: batching of _changes, _revs_diff, _bulk_docs?

    def __init__(self, url, credentials=None, *args, **kwargs):
        super().__init__(base_url=url, *args, **kwargs)

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
        try:
            base_id = (await self._request('GET', '../')).json()['uuid']
        except KeyError:
            base_id = ''
        return base_id + str(self.base_url) + 'remote'

    async def changes(self, since=None):
        params = {'style': 'all_docs'}
        if since is not None:
            params['since'] = since
        async with self._stream('GET', '/_changes', params=params) as resp:
            assert resp.status_code == httpx.codes.OK
            async for c in parse_json_stream(resp.aiter_bytes(), 'items',
                                             'results.item'):
                deleted = c.get('deleted', False)
                leaf_revs = [item['rev'] for item in c['changes']]
                yield Change(c['id'], c['seq'], deleted, leaf_revs)

    async def revs_diff(self, remote):
        body = self._revs_diff_body(remote)
        async with self._stream('POST', '/_revs_diff', data=body,
                                headers=JSON_REQ_HEADERS) as resp:
            assert resp.status_code == httpx.codes.OK

            async for id, info in parse_json_stream(resp.aiter_bytes(),
                                                    'kvitems', ''):
                yield id, info

    async def _revs_diff_body(self, remote):
        yield b'{'
        async for i, (id, revs) in aenumerate(remote):
            if i > 0:
                yield b','
            yield as_json(id).encode('UTF-8')
            yield b':'
            yield as_json(revs).encode('UTF-8')
        yield b'}\n'

    async def write(self, docs):
        body = self._bulk_docs_body(docs)
        async with self._stream('POST', '/_bulk_docs', data=body,
                                headers=JSON_REQ_HEADERS) as resp:
            assert resp.status_code == httpx.codes.CREATED
            async for row in parse_json_stream(resp.aiter_bytes(), 'items',
                                               'item'):
                yield row

    async def _bulk_docs_body(self, docs):
        yield b'{"new_edits":false,"docs":['
        async for i, doc in aenumerate(docs):
            if i > 0:
                yield b','
            yield as_json(doc).encode('UTF-8')
        yield b']}'

    async def read(self, requested, include_path=False):
        # the method for reading the docs in parallel is inspired by:
        # https://stackoverflow.com/a/55317623

        tasks = []
        queue = asyncio.Queue()
        async for id, revs in requested:
            params = self._read_params(id, revs, include_path)

            docs = self._read_doc(id, revs, params)
            tasks.append(asyncio.create_task(self._drain_into(docs, queue)))
            while len(tasks) >= MAX_PARALLEL_READS:
                yield await queue.get()
                tasks = [task for task in tasks if not task.done()]

        while tasks:
            yield await queue.get()
            tasks = [task for task in tasks if not task.done()]

    def _read_params(self, id, revs, include_path):
        params = {'latest': 'true'}
        if include_path:
            params['revs'] = 'true'
        if revs == 'all':
            params['open_revs'] = 'all'
        elif revs != 'winner':
            params['open_revs'] = as_json(revs)
        return params

    async def _drain_into(self, docs, queue):
        async for doc in docs:
            await queue.put(doc)

    async def _read_doc(self, id, revs, params):
        async with self._stream('GET', '/' + id, params=params) as resp:
            if resp.headers['Content-Type'] == 'application/json':
                await resp.aread()
                if resp.status_code == httpx.codes.NOT_FOUND:
                    yield NotFound(resp.json())
                else:
                    assert resp.status_code == httpx.codes.OK
                    yield resp.json()
            else:
                assert resp.status_code == httpx.codes.OK
                async for doc in self._read_multipart(resp):
                    yield doc

    async def _read_multipart(self, resp):
        async for part in MultipartResponseParser(resp):
            if part.headers['Content-Type'].startswith('multipart/related'):
                subparser = MultipartResponseParser(part)
                async for sub in subparser:
                    assert sub.headers == {'Content-Type': 'application/json'}
                    yield json.loads(await sub.aread())
                    # first item is the document. TODO: handle attachments
                    # instead of skipping them like this:
                    subparser.parser.change_state(subparser.parser.DONE)
                    break
            else:
                assert part.headers == {'Content-Type': 'application/json'}
                yield json.loads(await part.aread())

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
