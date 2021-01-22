import pytest

import asyncio
import contextlib
import databases
import pprint

from chairdb import (HTTPDatabase, InMemoryDatabase, SQLDatabase, replicate,
                     NotFound, app)


@pytest.mark.asyncio
@pytest.fixture
async def sql_target():
    async with databases.Database('sqlite:///test.sqlite3') as db:
        async with SQLDatabase(db) as target2:
            yield target2


@pytest.mark.asyncio
async def test_replicate(sql_target):
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

        async with HTTPDatabase('http://test/test', app=app) as local_server:
            result = await replicate(target, local_server, create_target=True)
            assert result['ok']
            result2 = await replicate(local_server, sql_target)
            assert result2['ok']
            result3 = await replicate(local_server, sql_target)
            assert result3['ok']
        target2 = InMemoryDatabase(id='another-test')
        result4 = await replicate(sql_target, target2)
        assert result4['ok']


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
