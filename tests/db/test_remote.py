import pytest

from chairdb import HTTPDatabase, Document
from chairdb.utils import async_iter

DOCS = [
    Document('mytest', 1, ('x',), {'Hello': 'World!'}),
    Document('mytest', 2, ('y', 'x'), body=None)
]

pytestmark = pytest.mark.anyio


async def test_remote_att():
    url = 'http://localhost:5984/brassbandwirdum'
    async with HTTPDatabase(url, credentials=('marten', 'test')) as db:
        async with db.read('_design/brassbandwirdum', atts_since=[]) as resp:
            async for doc in resp:
                print(doc.id, doc.rev_num, doc.path, doc.body.keys())
                for name, att in doc.attachments.items():
                    total_length = sum([len(chunk) async for chunk in att])
                    print(name, att.meta, total_length)


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
            async for result in db.write(async_iter(DOCS)):
                print(result)  # succesful writes don't return anything
            req = [
                # three different ways...
                ('mytest', {'revs': 'all'}),
                ('mytest', {}),
                ('mytest', {'revs': [(2, 'y')]}),
            ]
            for id, opts in req:
                async with db.read(id, **opts) as resp:
                    async for result in resp:
                        print(result)
            async for change in db.changes():
                print(change)
                break

            assert 'remote' in await db.id
        finally:
            # clean up
            print(await db.destroy())
