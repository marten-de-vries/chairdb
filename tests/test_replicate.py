import pytest

import pprint

from microcouch import HTTPDatabase, InMemoryDatabase, replicate
from microcouch.server import app

@pytest.mark.asyncio
async def test_replicate():
    import pprint

    # guarantee stable replication id
    target = InMemoryDatabase(id='test')
    async with HTTPDatabase('http://localhost:5984/brassbandwirdum',
                            credentials=('marten', 'test')) as source:
        # first time
        pprint.pprint(await replicate(source, target))
        # second time, should go much faster
        assert (await replicate(source, target))['ok']

    async with HTTPDatabase('http://localhost:5984/activiteitenweger',
                            credentials=('marten', 'test')) as source2:
        await replicate(source2, target)

    target2 = InMemoryDatabase(id='test2')
    async with HTTPDatabase('http://test/test', app=app) as local_server:
        result = await replicate(target, local_server, create_target=True)
        assert result['ok']
        result2 = await replicate(local_server, target2)
        assert result2['ok']
