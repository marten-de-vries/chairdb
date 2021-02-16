import pytest

import asyncio
import contextlib
import databases
import pprint

from chairdb import (HTTPDatabase, InMemoryDatabase, SQLDatabase, replicate,
                     NotFound, app, Document)


@pytest.mark.asyncio
@pytest.fixture
async def sql_target():
    async with databases.Database('sqlite:////dev/shm/test.sqlite3') as db:
        async with SQLDatabase(db) as target2:
            yield target2


@pytest.mark.asyncio
@pytest.fixture
async def brassbandwirdum():
    async with HTTPDatabase('http://localhost:5984/brassbandwirdum',
                            credentials=('marten', 'test')) as source:
        yield source


@pytest.mark.asyncio
@pytest.fixture
async def activiteitenweger():
    async with HTTPDatabase('http://localhost:5984/activiteitenweger',
                            credentials=('marten', 'test')) as source2:
        yield source2


@pytest.mark.asyncio
@pytest.fixture
async def local_server():
    async with HTTPDatabase('http://test/test', app=app) as server:
        yield server


@pytest.mark.asyncio
async def test_replicate_multi(sql_target, brassbandwirdum, activiteitenweger,
                               local_server):
    # guarantee stable replication id
    target = InMemoryDatabase(id='test')
    # first time
    pprint.pprint(await replicate(brassbandwirdum, target))
    # second time, should go much faster
    assert (await replicate(brassbandwirdum, target))['ok']
    # another database
    await replicate(activiteitenweger, target)

    # test local server as target
    result = await replicate(target, local_server, create_target=True)
    assert result['ok']
    # and as source (with sql as target)
    result2 = await replicate(local_server, sql_target)
    assert result2['ok']
    # again
    result3 = await replicate(local_server, sql_target)
    assert result3['ok']

    # use sql as source
    target2 = InMemoryDatabase(id='another-test')
    result4 = await replicate(sql_target, target2)
    assert result4['ok']


@pytest.mark.asyncio
async def test_replicate_continuous():
    source = InMemoryDatabase()
    source.write_sync(Document('test', 1, ('a',), {}))
    target = InMemoryDatabase()
    task = asyncio.create_task(replicate(source, target, continuous=True))
    # verify the 'normal' replication is done (everything in the db has been
    # replicated succesfully)
    await document_existance(target, Document('test', 1, ('a',), {}))
    # now write another document to check 'continuous=True'
    source.write_sync(Document('test2', 1, ('b',), {}))
    await document_existance(target, Document('test2', 1, ('b',), {}))
    # clean up
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def document_existance(db, doc):
    result = None
    while result != doc:
        try:
            result = next(db.read_sync(doc.id))
        except NotFound:
            await asyncio.sleep(0)
