import contextlib
import re

import aioitertools
import httpx
import ijson
import json

from .errors import Unauthorized, Forbidden, NotFound
from .utils import Change, as_json
from .multipart import MultipartParser


JSON_REQ_HEADERS = {'Content-Type': 'application/json'}


class HTTPDatabase(httpx.AsyncClient):
    # TODO: batch?

    def __init__(self, url, credentials=None, *args, **kwargs):
        super().__init__(base_url=url, *args, **kwargs)

        self._credentials = None
        if credentials:
            name, password = credentials
            self._credentials = {'name': name, 'password': password}

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
        if resp.status_code == httpx.codes.NOT_FOUND:
            raise NotFound(resp.json())
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

    @property
    def update_seq(self):
        return self._get_update_seq()

    async def _get_update_seq(self):
        return (await self._request('GET', '/')).json()['update_seq']

    @property
    def id(self):
        return self._get_id()

    async def _get_id(self):
        try:
            base_id = (await self._request('GET', '../')).json()['uuid']
        except KeyError:
            base_id = ''
        return base_id + str(self.base_url) + 'remote'

    async def create(self):
        resp = await self._request('PUT', '/')
        if resp.status_code == httpx.codes.PRECONDITION_FAILED:
            return False  # already exists
        assert resp.status_code == httpx.codes.CREATED
        return True

    async def destroy(self):
        return (await self._request('DELETE', '/')).json()

    async def changes(self):
        params = {'style': 'all_docs'}
        async with self._stream('GET', '/_changes', params=params) as resp:
            assert resp.status_code == httpx.codes.OK
            async for c in parse_json_stream(resp, 'items', 'results.item'):
                leaf_revs = [item['rev'] for item in c['changes']]
                yield Change(c['id'], c['seq'], c['deleted'], leaf_revs)

    async def revs_diff(self, remote):
        body = self._revs_diff_body(remote)
        async with self._stream('POST', '/_revs_diff', data=body,
                                headers=JSON_REQ_HEADERS) as resp:
            assert resp.status_code == httpx.codes.OK

            async for id, info in parse_json_stream(resp, 'kvitems', ''):
                yield id, info

    async def _revs_diff_body(self, remote):
        yield b'{'
        async for i, (id, revs) in aioitertools.enumerate(remote):
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
            async for row in parse_json_stream(resp, 'items', 'item'):
                yield row

    async def _bulk_docs_body(self, docs):
        yield b'{"new_edits":false,"docs":['
        async for i, doc in aioitertools.enumerate(docs):
            if i > 0:
                yield b','
            yield as_json(doc).encode('UTF-8')
        yield b']}'

    async def read(self, requested, include_path=False):
        # TODO: fetch in parallel
        async for id, revs in requested:
            params = {'revs': 'true'} if include_path else {}
            if revs == 'all':
                params['open_revs'] = 'all'
            elif revs != 'winner':
                params['open_revs'] = as_json(revs)
            async for doc in self._read_doc(id, revs, params):
                yield doc

    async def _read_doc(self, id, revs, params):
        try:
            async with self._stream('GET', '/' + id, params=params) as resp:
                assert resp.status_code == httpx.codes.OK
                if resp.headers['Content-Type'] == 'application/json':
                    await resp.aread()
                    yield resp.json()
                else:
                    async for doc in self._read_multipart(resp):
                        yield doc
        except NotFound as e:
            yield e

    async def _read_multipart(self, resp):
        type = resp.headers['Content-Type']
        match = re.match('multipart/mixed; boundary="([^"]+)"', type)
        assert match
        boundary = match[1].encode('UTF-8')
        parser = MultipartParser(boundary)
        async for chunk in resp.aiter_bytes():
            parser.feed(chunk)
            for headers, doc in parser.results:
                assert headers == {'Content-Type': 'application/json'}
                yield json.loads(doc)
            parser.results.clear()
        parser.check_done()


async def parse_json_stream(stream, type, prefix):
    results = ijson.sendable_list()
    coro = getattr(ijson, type + '_coro')(results, prefix)
    async for chunk in stream.aiter_bytes():
        with contextlib.suppress(StopIteration):
            coro.send(chunk)
        for result in results:
            yield result
        results.clear()
