import pytest
from microcouch import InMemoryDatabase, Change, NotFound
from microcouch.utils import async_iter, to_list


@pytest.fixture
def db():
    return InMemoryDatabase()


def insert_doc(db):
    doc = {'_id': 'test', '_rev': '1-a', 'hello': 'world'}
    db.write_sync(doc.copy())
    return doc


def test_simple(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test', ['1-a'])) == [doc]


def test_read_winner(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test', 'winner')) == [doc]


def test_read_winner_with_path(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test', 'winner', include_path=True)) == [
        {'_revisions': {'start': 1, 'ids': ['a']}, **doc}
    ]


def test_read_all(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test', 'all')) == [doc]


def test_revs_diff(db):
    doc = insert_doc(db)
    assert db.revs_diff_sync('test', ['1-a', '1-b']) == ('test', {
        'missing': {'1-b'},
    })
    assert db.revs_diff_sync('unexisting', ['1-c']) == ('unexisting', {
        'missing': {'1-c'}
    })


def test_changes(db):
    insert_doc(db)
    assert list(db.changes_sync()) == [
        Change('test', seq=1, deleted=False, leaf_revs=['1-a'])
    ]


def test_overwrite(db):
    doc = insert_doc(db)
    db.write_sync({'_id': 'test', '_rev': '2-a', 'hello': 'everyone'})
    assert list(db.read_sync('test', 'winner')) == [
        {'_id': 'test', '_rev': '2-a', 'hello': 'everyone'},
    ]


def test_linear_history(db):
    doc = insert_doc(db)
    docs = [
        {'_id': 'test', '_rev': '2-b', 'hello': '1', '_revisions': {
            'start': 2,
            'ids': ['b', 'a'],
        }},
        {'_id': 'test', '_rev': '3-c', 'hello': '2', '_revisions': {
            'start': 3,
            'ids': ['c', 'b', 'a'],
        }},
        # skip one
        {'_id': 'test', '_rev': '5-e', 'hello': '4', '_revisions': {
            'start': 5,
            'ids': ['e', 'd', 'c', 'b', 'a'],
        }}
    ]
    for doc in docs:
        db.write_sync(doc)
    assert list(db.read_sync('test', 'all')) == [
        {'_id': 'test', '_rev': '5-e', 'hello': '4'}
    ]


def test_remove(db):
    doc = insert_doc(db)
    doc2 = {'_id': 'test', '_rev': '2-b', '_deleted': True, 'other': 'data',
            '_revisions': {'start': 2, 'ids': ['b', 'a']}}
    db.write_sync(doc2)
    assert list(db.read_sync('test', 'winner')) == [
        {'_id': 'test', '_rev': '2-b', '_deleted': True},
    ]
    assert list(db.changes_sync()) == [
        Change('test', seq=2, deleted=True, leaf_revs=['2-b'])
    ]


def test_conflict(db):
    db.write_sync({'_id': 'test', '_rev': '1-a', 'hello': 'world'})
    db.write_sync({'_id': 'test', '_rev': '1-b', 'hello': 'there'})
    assert list(db.read_sync('test', 'all')) == [
        {'_id': 'test', '_rev': '1-b', 'hello': 'there'},
        {'_id': 'test', '_rev': '1-a', 'hello': 'world'},
    ]


def test_reinsert(db):
    insert_doc(db)
    insert_doc(db)


def test_old_conflict(db):
    docs = [
        {'_id': 'test', '_rev': '1-a', 'x': 1},
        {'_id': 'test', '_rev': '2-b', 'x': 2, '_revisions': {
            'start': 2,
            'ids': ['b', 'a'],
        }},
        {'_id': 'test', '_rev': '3-c', 'x': 3, '_revisions': {
            'start': 3,
            'ids': ['c', 'b', 'a'],
        }},
        # the interesting one (the old conflict):
        {'_id': 'test', '_rev': '2-d', 'x': 4, '_revisions': {
            'start': 2,
            'ids': ['d', 'a'],
        }},
    ]
    for doc in docs:
        db.write_sync(doc)
    # make sure both leafs are in there
    assert list(db.read_sync('test', 'all', include_path=True)) == [
        {'_id': 'test', '_rev': '3-c', 'x': 3, '_revisions': {
            'start': 3,
            'ids': ['c', 'b', 'a'],
        }},
        {'_id': 'test', '_rev': '2-d', 'x': 4, '_revisions': {
            'start': 2,
            'ids': ['d', 'a'],
        }},
    ]

    # make sure the older leaf is retrievable
    assert list(db.read_sync('test', ['2-d'])) == [
        {'_id': 'test', '_rev': '2-d', 'x': 4}
    ]

    # remove current winner
    db.write_sync({
        '_id': 'test',
        '_rev': '4-e',
        '_deleted': True,
        '_revisions': {'start': 4, 'ids': ['e', 'c', 'b', 'a']}
    })

    # check the winner is the 'old' leaf now.
    assert list(db.read_sync('test', 'winner')) == [
        {'_id': 'test', '_rev': '2-d', 'x': 4}
    ]

    # remove the remaining non-deleted leaf as well
    db.write_sync({
        '_id': 'test',
        '_rev': '3-f',
        '_deleted': True,
        '_revisions': {'start': 3, 'ids': ['f', 'd', 'a']}
    })

    # check if the current winner is deleted - and has switched back again
    assert list(db.read_sync('test', 'winner')) == [
        {'_id': 'test', '_rev': '4-e', '_deleted': True}
    ]


def test_sync(db):
    doc = {'_id': '_local/test', 'hello': 'world!'}
    db.write_sync(doc)
    assert list(db.read_sync('_local/test', 'winner')) == [
        {'_id': '_local/test', '_rev': '0-1', 'hello': 'world!'}
    ]
    db.write_sync({'_id': '_local/test', '_deleted': True})
    with pytest.raises(NotFound):
        next(db.read_sync('_local/test', 'winner'))


@pytest.mark.asyncio
async def test_async(db):
    assert await db.update_seq == 0
    assert await to_list(db.revs_diff(async_iter([
        ('unexisting', ['1-x', '2-y'])
    ]))) == [
        ('unexisting', {'missing': {'1-x', '2-y'}}),
    ]
    # query some unexisting rev
    docs = [
        {'_id': 'mytest', '_rev': '1-x', 'Hello': 'World!', '_revisions': {
            'start': 1,
            'ids': ['x'],
        }},
        {'_id': 'mytest', '_rev': '2-y', '_deleted': True, '_revisions': {
            'start': 2,
            'ids': ['y', 'x']
        }},
        {}
    ]
    result = await to_list(db.write(async_iter(docs)))
    assert len(result) == 1 and isinstance(result[0], KeyError)
    req = [
        # three different ways...
        ('mytest', 'all'),
        ('mytest', 'winner'),
        ('mytest', ['2-y']),
    ]
    assert await to_list(db.read(async_iter(req))) == [
        {'_id': 'mytest', '_rev': '2-y', '_deleted': True}
    ] * 3
    assert await to_list(db.changes()) == [
        Change('mytest', seq=2, deleted=True, leaf_revs=['2-y'])
    ]
    # try never-existing doc
    errors = await to_list(db.read(async_iter([('abc', 'winner')])))
    assert len(errors) == 1 and isinstance(errors[0], NotFound)

    assert 'memory' in await db.id
