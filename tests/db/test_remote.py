import pytest

from chairdb import HTTPDatabase, Document, anext
from chairdb.utils import async_iter

DOCS = [
    Document('mytest', 1, ('x',), {'Hello': 'World!'}),
    Document('mytest', 2, ('y', 'x'), is_deleted=True)
]

pytestmark = pytest.mark.anyio


async def test_remote_att():
    url = 'http://localhost:5984/brassbandwirdum'
    async with HTTPDatabase(url, credentials=('marten', 'test')) as db:
        async with db.read_with_attachments('_design/brassbandwirdum',
                                            atts_since=[]) as resp:
            async for doc in resp:
                for name, att in doc.attachments.items():
                    total_length = sum([len(chunk) async for chunk in att])
                    assert total_length == int(att.meta.length)


async def test_remote():  # noqa: C901
    url = 'http://localhost:5984/test'
    async with HTTPDatabase(url, credentials=('marten', 'test')) as db:
        try:
            assert await db.create()
            assert not await db.create()

            assert await db.update_seq
            # query some unexisting rev
            query = [('unexisting', [(1, 'x'), (2, 'y')]), ('abc', [(1, 'a')])]
            async for change in db.revs_diff(async_iter(query)):
                print(change)
            for doc in DOCS:
                await db.write(doc)
            req = [
                # three different ways...
                ('mytest', {'revs': 'all'}),
                ('mytest', {}),
                ('mytest', {'revs': [(2, 'y')]}),
            ]
            for id, opts in req:
                async for result in db.read(id, **opts):
                    print(result)
            print(await anext(db.changes()))

            assert 'remote' in await db.id
        finally:
            # clean up
            print(await db.destroy())
