import contextlib

import asyncio
import httpx
import json
import typing

from .datatypes import (Unauthorized, Forbidden, NotFound, Change, Missing,
                        AttachmentMetadata)
from ..multipart import MultipartStreamParser
from ..utils import (as_json, parse_json_stream, parse_rev, json_array_inner,
                     rev, couchdb_json_to_doc, doc_to_couchdb_json,
                     json_object_inner)


JSON_REQ_HEADERS = {'Content-Type': 'application/json'}
MAX_PARALLEL_READS = 10
FIRST_DONE = asyncio.FIRST_COMPLETED


class HTTPAttachment(typing.NamedTuple):
    meta: AttachmentMetadata
    iterator: asyncio.Future
    is_stub: bool = False

    async def __aiter__(self):
        async for chunk in await self.iterator:
            yield chunk


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
                # TODO: handle timeouts
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

    async def write(self, docs):
        header = '{"new_edits":false,"docs":['
        docs_json = self._bulk_docs_json(docs)
        body = json_array_inner(header, docs_json, lambda: ']}\n')
        body_bytes = self._encode_aiter(body)
        async with self._stream('POST', '/_bulk_docs', data=body_bytes,
                                headers=JSON_REQ_HEADERS) as resp:
            assert resp.status_code == httpx.codes.CREATED
            async for row in parse_json_stream(resp.aiter_bytes(), 'items',
                                               'item'):
                yield row

    async def _bulk_docs_json(self, docs):
        async for doc in docs:
            yield as_json(await doc_to_couchdb_json(doc))

    async def write_local(self, id, doc):
        await self._request('PUT', f'/_local/{id}', json=doc)

    # FOR EASIER DEBUGGING:
    # async def read(self, requested):
    #     async for id, *args in requested:
    #         params = self._read_params(id, *args)
    #         async for doc in self._read_docs(id, params):
    #             yield doc

    async def read(self, requested):
        # the method for reading the docs in parallel is inspired by:
        # https://stackoverflow.com/a/55317623

        queue = asyncio.Queue()
        read_task = asyncio.create_task(self._read_task(requested, queue))

        # keep reading until done, but also quickly return results as they
        # become available.
        while True:
            get_task = asyncio.create_task(queue.get())
            done, pending = await asyncio.wait([read_task, get_task],
                                               return_when=FIRST_DONE)
            if get_task in done:
                yield await get_task
            if read_task in done:
                get_task.cancel()
                break
        # reading is done, empty out the queue.
        while not queue.empty():
            yield queue.get_nowait()

    async def _read_task(self, requested, queue):
        # read MAX_PARALLEL_READS docs at the same time. Results are put into
        # 'queue'
        tasks = set()
        async for id, *args in requested:
            params = self._read_params(id, *args)
            docs = self._read_docs(id, params)

            tasks.add(asyncio.create_task(self._drain_into(docs, queue)))
            while len(tasks) >= MAX_PARALLEL_READS:
                _, tasks = await asyncio.wait(tasks, return_when=FIRST_DONE)
        await asyncio.wait(tasks)

    def _read_params(self, id, revs, att_names=None, atts_since=None):
        assert att_names is None  # TODO
        params = {'latest': 'true', 'revs': 'true'}
        if revs == 'all':
            params['open_revs'] = 'all'
        elif revs != 'winner':
            params['open_revs'] = as_json([rev(*r) for r in revs])
        if atts_since:
            params['atts_since'] = as_json([rev(*r) for r in atts_since])
        return params

    async def _drain_into(self, docs, queue):
        async for doc in docs:
            await queue.put(doc)

    async def _read_docs(self, id, params):
        async with self._stream('GET', '/' + id, params=params) as resp:
            if resp.headers['Content-Type'] == 'application/json':
                await resp.aread()
                if resp.status_code == httpx.codes.NOT_FOUND:
                    yield NotFound(resp.json())
                else:
                    assert resp.status_code == httpx.codes.OK
                    doc, todo = couchdb_json_to_doc(resp.json())
                    assert not todo
                    yield doc
            else:
                assert resp.status_code == httpx.codes.OK
                async for doc in self._read_multipart(resp):
                    yield doc

    async def _read_multipart(self, resp):
        async for part in MultipartStreamParser(resp):
            if part.headers['Content-Type'].startswith('multipart/related'):
                subparser = MultipartStreamParser(part).__aiter__()
                sub = await subparser.__anext__()
                assert sub.headers == {'Content-Type': 'application/json'}
                doc, todo = couchdb_json_to_doc(json.loads(await sub.aread()))
                futures = {}
                for name, meta in todo:
                    futures[name] = asyncio.get_event_loop().create_future()
                    doc.attachments[name] = HTTPAttachment(meta, futures[name])
                yield doc
                # TODO: does the following need to happen in a separate task?
                async for att in subparser:
                    _, name, _ = att.headers['Content-Disposition'].split('"')
                    futures[name].set_result(att.aiter_bytes())
            else:
                assert part.headers == {'Content-Type': 'application/json'}
                doc, todo = couchdb_json_to_doc(json.loads(await part.aread()))
                assert not todo
                yield doc

    async def read_local(self, id):
        doc = await self._read_docs('_local/' + id, {}).__anext__()
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
