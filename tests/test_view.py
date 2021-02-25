from chairdb import InMemoryDatabase, View, Document
from chairdb.utils import to_list
from chairdb.view import QueryResult

import pytest

pytestmark = pytest.mark.anyio


def map_title(doc):
    yield doc['title'], None


async def test_view():
    db = InMemoryDatabase()
    view = View(db, map_title)

    await db.write(Document('test', 1, ('a',), {'title': 'mytest'}))
    await db.write(Document('test2', 1, ('b',), {'title': 'Hello World'}))
    assert await to_list(view.query()) == [
        QueryResult(key='Hello World', value=None, id='test2', document=None),
        QueryResult(key='mytest', value=None, id='test', document=None)
    ]
    assert await to_list(view.query(start_key='m', end_key='n')) == [
        QueryResult(key='mytest', value=None, id='test', document=None)
    ]
    await db.write(Document('test2', 2, ('c', 'b'), {'title': 'Hello World!'}))
    assert await to_list(view.query()) == [
        QueryResult(key='Hello World!', value=None, id='test2', document=None),
        QueryResult(key='mytest', value=None, id='test', document=None)
    ]
