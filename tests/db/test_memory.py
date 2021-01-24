import pytest
from chairdb import InMemoryDatabase, Change, NotFound, Document, Missing
from chairdb.utils import async_iter, to_list


@pytest.fixture
def db():
    return InMemoryDatabase()


def insert_doc(db):
    doc = Document('test', 1, ['a'], {'hello': 'world'})
    db.write_sync(doc)
    return doc


def test_simple(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test', [(1, 'a')])) == [doc]


def test_read_winner(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test', 'winner')) == [doc]


def test_read_all(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test', 'all')) == [doc]


def test_revs_diff(db):
    insert_doc(db)
    m1 = Missing('test', {(1, 'b')})
    assert db.revs_diff_sync('test', [(1, 'a'), (1, 'b')]) == m1
    m2 = Missing('unexisting', {(1, 'c')})
    assert db.revs_diff_sync('unexisting', [(1, 'c')]) == m2


def test_changes(db):
    insert_doc(db)
    assert list(db.changes_sync()) == [
        Change('test', seq=1, deleted=False, leaf_revs=[(1, 'a')])
    ]


def test_overwrite(db):
    insert_doc(db)
    db.write_sync(Document('test', 2, ['a'], {'hello everyone'}))
    assert list(db.read_sync('test', 'winner')) == [
        Document('test', 2, ['a'], {'hello everyone'})
    ]


def test_linear_history(db):
    doc = insert_doc(db)
    docs = [
        Document('test', 2, ['b', 'a'], {'hello': '1'}),
        Document('test', 3, ['c', 'b', 'a'], {'hello': '2'}),
        # skip one
        Document('test', 5, ['e', 'd', 'c', 'b', 'a'], {'hello': '4'})
    ]
    for doc in docs:
        db.write_sync(doc)
    assert list(db.read_sync('test', 'all')) == [
        Document('test', 5, ['e', 'd', 'c', 'b', 'a'], {'hello': '4'})
    ]


def test_remove(db):
    insert_doc(db)
    doc2 = Document('test', 2, ['b', 'a'])
    db.write_sync(doc2)
    assert list(db.read_sync('test', 'winner')) == [doc2]
    assert list(db.changes_sync()) == [
        Change('test', seq=2, deleted=True, leaf_revs=[(2, 'b')])
    ]


def test_conflict(db):
    db.write_sync(Document('test', 1, ['a'], {'hello': 'world'}))
    db.write_sync(Document('test', 1, ['b'], {'hello': 'there'}))
    assert list(db.read_sync('test', 'all')) == [
        Document('test', 1, ['b'], {'hello': 'there'}),
        Document('test', 1, ['a'], {'hello': 'world'}),
    ]


def test_reinsert(db):
    insert_doc(db)
    insert_doc(db)


def test_old_conflict(db):
    docs = [
        Document('test', 1, ['a'], {'x': 1}),
        Document('test', 2, ['b', 'a'], {'x': 2}),
        Document('test', 3, ['c', 'b', 'a'], {'x': 3}),
        # the interesting one (the old conflict):
        Document('test', 2, ['d', 'a'], {'x': 4}),
    ]
    for doc in docs:
        db.write_sync(doc)
    # make sure both leafs are in there
    assert list(db.read_sync('test', 'all')) == [
        Document('test', 3, ['c', 'b', 'a'], {'x': 3}),
        Document('test', 2, ['d', 'a'], {'x': 4}),
    ]

    # make sure the older leaf is retrievable
    assert list(db.read_sync('test', [(2, 'd')])) == [
        Document('test', 2, ['d', 'a'], {'x': 4}),
    ]

    # remove current winner
    db.write_sync(Document('test', 4, ['e', 'c', 'b', 'a']))

    # check the winner is the 'old' leaf now.
    assert list(db.read_sync('test', 'winner')) == [
        Document('test', 2, ['d', 'a'], {'x': 4}),
    ]

    # remove the remaining non-deleted leaf as well
    db.write_sync(Document('test', 3, ['f', 'd', 'a']))

    # check if the current winner is deleted - and has switched back again
    assert list(db.read_sync('test', 'winner')) == [
        Document('test', 4, ['e', 'c', 'b', 'a']),
    ]


def test_sync(db):
    # local documents
    doc = Document('test', body={'hello': 'world!'})
    db.write_sync(doc)
    assert list(db.read_sync('test', None)) == [
        doc
    ]
    db.write_sync(Document('test', body=None))
    with pytest.raises(NotFound):
        next(db.read_sync('test', None))


@pytest.mark.asyncio
async def test_async(db):
    assert await db.update_seq == 0
    # query some unexisting rev
    assert await to_list(db.revs_diff(async_iter([
        ('unexisting', [(1, 'x'), (2, 'y')])
    ]))) == [
        Missing('unexisting', {(1, 'x'), (2, 'y')}),
    ]
    docs = [
        Document('mytest', 1, ['x'], {'Hello': 'World!'}),
        Document('mytest', 2, ['y', 'x']),
        {}  # not a document
    ]
    result = await to_list(db.write(async_iter(docs)))
    assert len(result) == 1 and isinstance(result[0], AttributeError)
    req = [
        # three different ways...
        ('mytest', 'all'),
        ('mytest', 'winner'),
        ('mytest', [(2, 'y')]),
    ]
    assert await to_list(db.read(async_iter(req))) == [
        Document('mytest', 2, ['y', 'x']),
    ] * 3
    assert await to_list(db.changes()) == [
        Change('mytest', seq=2, deleted=True, leaf_revs=[(2, 'y')])
    ]
    # try never-existing doc
    errors = await to_list(db.read(async_iter([('abc', 'winner')])))
    assert len(errors) == 1 and isinstance(errors[0], NotFound)

    assert 'memory' in await db.id

    assert await db.revs_limit == 1000
    await db.set_revs_limit(500)
    assert await db.revs_limit == 500
