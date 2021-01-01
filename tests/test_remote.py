import pytest

from microcouch import HTTPDatabase

@pytest.mark.asyncio
async def test_remote():
    url = 'http://localhost:5984/test'
    async with HTTPDatabase(url, auth=('marten', 'test')) as db:
        print(await db.create())
        print(await db.update_seq)
        async for change in db.revs_diff({'unexisting': ['1-x', '2-y']}):
            print(change)
        # query some unexisting rev
        docs = [
            {'_id': 'mytest', '_rev': '1-x', 'Hello': 'World!', '_revisions': {
                'start': 1,
                'ids': ['x'],
            }},
            {'_id': 'mytest', '_rev': '2-y', '_deleted': True, '_revisions': {
                'start': 2,
                'ids': ['y', 'x']
            }}
        ]
        async for result in db.write(docs):
            # empty for couchdb, which only sends back errors, not successes
            print(result)
        req = [
            # three different ways...
            ('mytest', 'all'),
            ('mytest', 'winner'),
            ('mytest', ['2-y']),
        ]
        async for result in db.read(req):
            print(result)
        async for change in db.changes():
            print(change)
            break
        # clean up
        print(await db.destroy())
