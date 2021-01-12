import pytest

import asyncio
import contextlib
import pprint

from microcouch import HTTPDatabase, InMemoryDatabase, replicate, NotFound
from microcouch.server import app


@pytest.mark.asyncio
async def test_replicate():
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


@pytest.mark.asyncio
async def test_replicate_continuous():
    source = InMemoryDatabase()
    source.write_sync({'_id': 'test', '_rev': '1-a'})
    target = InMemoryDatabase()
    task = asyncio.create_task(replicate(source, target, continuous=True))
    # verify the 'normal' replication is done (everything in the db has been
    # replicated succesfully)
    await document_existance(target, {'_id': 'test', '_rev': '1-a'})
    # now write another document to check 'continuous=True'
    source.write_sync({'_id': 'test2', '_rev': '1-b'})
    await document_existance(target, {'_id': 'test2', '_rev': '1-b'})
    # clean up
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def document_existance(db, doc):
    result = None
    while result != doc:
        try:
            result = next(db.read_sync(doc['_id'], 'winner'))
        except NotFound:
            await asyncio.sleep(0)
