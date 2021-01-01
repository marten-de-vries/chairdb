import asyncio
import httpx


class HTTPDatabase(httpx.AsyncClient):
    # TODO: batch?

    def __init__(self, url, auth=None):
        super().__init__(base_url=url)

        self._credentials = None
        if auth:
            self._credentials = {'name': auth[0], 'password': auth[1]}

    @property
    def update_seq(self):
        return self._get_update_seq()

    async def _get_update_seq(self):
        await self._ensure_login()

        return (await self.get('/')).json()['update_seq']

    async def create(self):
        await self._ensure_login()

        return (await self.put('/')).json()

    async def destroy(self):
        await self._ensure_login()

        return (await self.delete('/')).json()

    async def _ensure_login(self):
        if self._credentials:
            # TODO: not so nice, maybe?
            resp = await self.post('../_session', json=self._credentials)
            assert resp.status_code == 200
            self._credentials = None

    async def changes(self):
        await self._ensure_login()
        resp = await self.get('/_changes', params={'style': 'all_docs'})
        assert resp.status_code == 200
        for change in resp.json()['results']:
            yield change

    async def revs_diff(self, remote):
        await self._ensure_login()
        response = await self.post('/_revs_diff', json=remote)
        assert response.status_code == 200
        for id, info in response.json().items():
            yield id, info

    async def write(self, docs):
        await self._ensure_login()
        payload = {'docs': list(docs), 'new_edits': False}
        resp = await self.post('/_bulk_docs', json=payload)
        assert resp.status_code == 201
        for row in resp.json():
            yield row

    async def read(self, requested, include_path=False):
        await self._ensure_login()
        individual = []
        docs = []
        params = {'revs': 'true'} if include_path else {}
        for id, revs in requested:
            if revs == 'winner':
                individual.append(id)
            elif revs == 'all':
                docs.append({'id': id})
            else:
                for rev in revs:
                    docs.append({'id': id, 'rev': rev})
        bulk_get = self.post('/_bulk_get', params=params, json={'docs': docs})
        others = [self.get(id, params=params) for id in individual]
        resp, *other_resps = await asyncio.gather(bulk_get, *others)
        assert resp.status_code == 200
        for collection in resp.json()['results']:
            for doc in collection['docs']:
                yield doc['ok']
        for id, other_resp in zip(individual, other_resps):
            doc = other_resp.json()
            if other_resp.status_code == 404:
                doc['id'] = id
            else:
                assert other_resp.status_code == 200
            yield doc
