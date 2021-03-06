import anyio
import pytest

import contextlib
import os
import pprint

from chairdb import (HTTPDatabase, InMemoryDatabase, SQLDatabase, replicate,
                     NotFound, app, Document, anext, sqlite_pool,
                     AttachmentSelector)

pytestmark = pytest.mark.anyio


@pytest.fixture
async def sql_target():
    with contextlib.suppress(FileNotFoundError):
        os.remove('/dev/shm/test.sqlite3')
    async with sqlite_pool('/dev/shm/test.sqlite3') as pool:
        target2 = SQLDatabase(pool)
        await target2.create()
        yield target2


@pytest.fixture
async def brassbandwirdum():
    async with HTTPDatabase('http://localhost:5984/brassbandwirdum',
                            credentials=('marten', 'test')) as source:
        yield source


@pytest.fixture
async def activiteitenweger():
    async with HTTPDatabase('http://localhost:5984/activiteitenweger',
                            credentials=('marten', 'test')) as source2:
        yield source2


@pytest.fixture
async def local_server():
    async with HTTPDatabase('http://test/test', app=app) as server:
        yield server


@pytest.mark.parametrize('anyio_backend', ['asyncio'])
async def test_replicate_multi(anyio_backend, sql_target, brassbandwirdum,
                               activiteitenweger, local_server):
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
    all_atts = AttachmentSelector.all()
    async with target2.read_with_attachments('_design/brassbandwirdum',
                                             atts=all_atts) as resp:
        doc = await anext(resp)
        print(doc.attachments.keys())


async def test_replicate_continuous():
    source = InMemoryDatabase()
    await source.write(Document('test', 1, ('a',), {}))
    target = InMemoryDatabase()
    async with anyio.create_task_group() as tg:
        create_target, continuous = False, True
        tg.start_soon(replicate, source, target, create_target, continuous)
        # verify the 'normal' replication is done (everything in the db has
        # been replicated succesfully)
        await document_existance(target, Document('test', 1, ('a',), {}))
        # now write another document to check 'continuous=True'
        await source.write(Document('test2', 1, ('b',), {}))
        await document_existance(target, Document('test2', 1, ('b',), {}))
        # clean up
        tg.cancel_scope.cancel()


async def document_existance(db, doc):
    result = None
    while result != doc:
        try:
            result = await anext(db.read(doc.id))
        except NotFound:
            await anyio.sleep(0)
