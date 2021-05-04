from chairdb import InMemoryDatabase, View, Document
from chairdb.utils import to_list
from chairdb.view import QueryResult

import pytest

pytestmark = pytest.mark.anyio


def map_title(doc):
    yield doc['title'], None


def reduce(keys, values):
    return len(values)


def rereduce(values):
    return sum(values)


async def test_view():
    db = InMemoryDatabase()
    view = View(db, map_title, reduce, rereduce)

    doc1 = Document('test', 1, ('a',), {'title': 'mytest'})
    doc2 = Document('test2', 1, ('b',), {'title': 'Hello World'})
    await db.write(doc1)
    await db.write(doc2)
    assert await to_list(view.query()) == [
        QueryResult(key='Hello World', value=None, id='test2', document=None),
        QueryResult(key='mytest', value=None, id='test', document=None)
    ]
    assert await to_list(view.query(start_key='m', end_key='n')) == [
        QueryResult(key='mytest', value=None, id='test', document=None)
    ]
    doc3 = Document('test2', 2, ('c', 'b'), {'title': 'Hello World!'})
    await db.write(doc3)
    assert await to_list(view.query()) == [
        QueryResult(key='Hello World!', value=None, id='test2', document=None),
        QueryResult(key='mytest', value=None, id='test', document=None)
    ]
    # with document
    assert await to_list(view.query(doc_opts={})) == [
        QueryResult(key='Hello World!', value=None, id='test2', document=doc3),
        QueryResult(key='mytest', value=None, id='test', document=doc1),
    ]
    assert await to_list(view.aggregate()) == [(None, 2)]

    # maybe a list example would be better
    group_result = [('He', 1), ('my', 1)]
    assert await to_list(view.aggregate(group_level=2)) == group_result

    assert await to_list(view.aggregate(start_key='z')) == [(None, 0)]
