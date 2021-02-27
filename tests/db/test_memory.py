import pytest

import random

from chairdb import (InMemoryDatabase, NotFound, PreconditionFailed, Document,
                     AttachmentMetadata)
from chairdb.db.datatypes import AttachmentStub, Change, Missing
from chairdb.utils import async_iter, to_list, anext, hashabledict


@pytest.fixture
def db():
    return InMemoryDatabase()


def insert_doc(db):
    doc = Document('test', 1, ('a',), {'hello': 'world'})
    db.write_sync(doc)
    return doc


def test_simple(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test', revs=[(1, 'a')])) == [doc]


def test_read_winner(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test')) == [doc]


def test_read_all(db):
    doc = insert_doc(db)
    assert list(db.read_sync('test', revs='all')) == [doc]


def test_revs_diff(db):
    insert_doc(db)
    m1 = Missing('test', {(2, 'b')}, {(1, 'a')})
    assert db.revs_diff_sync('test', [(1, 'a'), (2, 'b')]) == m1
    m2 = Missing('unexisting', {(1, 'c')}, set())
    assert db.revs_diff_sync('unexisting', [(1, 'c')]) == m2


def test_all_docs(db):
    doc = insert_doc(db)
    assert list(db.all_docs_sync(doc_opts={'body': True})) == [doc]


def test_changes(db):
    insert_doc(db)
    assert list(db.changes_sync()) == [
        Change('test', seq=1, deleted=False, leaf_revs=[(1, 'a')])
    ]


def test_overwrite(db):
    insert_doc(db)
    db.write_sync(Document('test', 2, ('a',), {'hello everyone'}))
    assert list(db.read_sync('test')) == [
        Document('test', 2, ('a',), {'hello everyone'})
    ]


def test_linear_history(db):
    doc = insert_doc(db)
    docs = [
        Document('test', 2, ('b', 'a'), {'hello': '1'}),
        Document('test', 3, ('c', 'b', 'a'), {'hello': '2'}),
        # skip one
        Document('test', 5, ('e', 'd', 'c', 'b', 'a'), {'hello': '4'})
    ]
    for doc in docs:
        db.write_sync(doc)
    assert list(db.read_sync('test', revs='all')) == [
        Document('test', 5, ('e', 'd', 'c', 'b', 'a'), {'hello': '4'})
    ]


def test_remove(db):
    insert_doc(db)
    doc2 = Document('test', 2, ('b', 'a'), is_deleted=True)
    db.write_sync(doc2)
    assert list(db.read_sync('test')) == [doc2]
    assert list(db.changes_sync()) == [
        Change('test', seq=2, deleted=True, leaf_revs=[(2, 'b')])
    ]


def test_conflict(db):
    db.write_sync(Document('test', 1, ('a',), {'hello': 'world'}))
    db.write_sync(Document('test', 1, ('b',), {'hello': 'there'}))
    assert list(db.read_sync('test', revs='all')) == [
        Document('test', 1, ('b',), {'hello': 'there'}),
        Document('test', 1, ('a',), {'hello': 'world'}),
    ]


def test_reinsert(db):
    insert_doc(db)
    insert_doc(db)


def test_old_conflict(db):
    docs = [
        Document('test', 1, ('a',), {'x': 1}),
        Document('test', 2, ('b', 'a'), {'x': 2}),
        Document('test', 3, ('c', 'b', 'a'), {'x': 3}),
        # the interesting one (the old conflict):
        Document('test', 2, ('d', 'a'), {'x': 4}),
    ]
    for doc in docs:
        db.write_sync(doc)
    # make sure both leafs are in there
    assert list(db.read_sync('test', revs='all')) == [
        Document('test', 3, ('c', 'b', 'a'), {'x': 3}),
        Document('test', 2, ('d', 'a'), {'x': 4}),
    ]

    # make sure the older leaf is retrievable
    assert list(db.read_sync('test', revs=[(2, 'd')])) == [
        Document('test', 2, ('d', 'a'), {'x': 4}),
    ]

    # remove current winner
    db.write_sync(Document('test', 4, ('e', 'c', 'b', 'a'), is_deleted=True))

    # check the winner is the 'old' leaf now.
    assert list(db.read_sync('test')) == [
        Document('test', 2, ('d', 'a'), {'x': 4}),
    ]

    # remove the remaining non-deleted leaf as well
    db.write_sync(Document('test', 3, ('f', 'd', 'a'), is_deleted=True))

    # check if the current winner is deleted - and has switched back again
    assert list(db.read_sync('test')) == [
        Document('test', 4, ('e', 'c', 'b', 'a'), is_deleted=True),
    ]


def test_sync(db):
    # local documents
    doc = {'hello': 'world!'}
    db.write_local_sync('test', doc)
    assert db.read_local_sync('test') == doc
    db.write_local_sync('test', None)
    assert db.read_local_sync('test') is None


def test_attachment(db):
    doc = Document('test', 1, ('a',), {'test': 1})
    doc.add_attachment('text.txt', [b'Hello World!'])
    db.write_sync(doc)

    new_doc = next(db.read_sync('test'))
    assert new_doc.id == 'test'
    assert new_doc.rev_num == 1
    assert new_doc.path == ('a',)
    assert new_doc.body == {'test': 1}

    meta1 = AttachmentMetadata(1, 'text/plain', 12,
                               'md5-7Qdih1MuhjZehB6Sv8UNjA==')
    assert new_doc.attachments == {'text.txt': AttachmentStub(meta1)}

    new_doc.rev_num = 2
    new_doc.path = ('b', 'a')
    new_doc.add_attachment('test.json', [b'{}'], 'application/json+special')
    db.write_sync(new_doc)

    read_doc = next(db.read_sync('test'))
    meta2 = AttachmentMetadata(2, 'application/json+special', 2,
                               'md5-mZFLkyvTelC5g8XnyQrpOw==')
    assert read_doc.attachments == {
        'text.txt': AttachmentStub(meta1),
        'test.json': AttachmentStub(meta2),
    }
    assert all(a.is_stub for a in read_doc.attachments.values())

    # retrieve text.txt by name
    with db.read_with_attachments_sync('test', att_names=['text.txt']) as resp:
        read_doc2 = next(resp)
        assert b''.join(read_doc2.attachments['text.txt']) == b'Hello World!'

    # retrieve test.json because it's newer
    with db.read_with_attachments_sync('test', revs='all',
                                       atts_since=[(1, 'a')]) as resp:
        read_doc3 = next(resp)
        assert b''.join(read_doc3.attachments['test.json']) == b'{}'
        assert read_doc3.attachments['text.txt'].is_stub

    # retrieve both
    with db.read_with_attachments_sync('test', atts_since=[]) as resp:
        read_doc4 = next(resp)
        assert not any(a.is_stub for a in read_doc4.attachments.values())
        assert b''.join(read_doc3.attachments['test.json']) == b'{}'


def test_attachment_errors(db):
    doc = Document('test', 1, ('a',), {}, attachments={
        'mytest': AttachmentStub(AttachmentMetadata(1, 'ct', 0, 'no hash...')),
    })
    with pytest.raises(PreconditionFailed):  # cannot be a stub
        db.write_sync(doc)
    # retry
    doc.add_attachment('mytest', [b''])
    db.write_sync(doc)
    doc.rev_num = 2
    doc.path = ('b', 'a')
    doc.attachments = {
        # note that the rev_pos (2) is wrong!
        'mytest': AttachmentStub(AttachmentMetadata(2, 'ct', 0, 'no hash')),
    }
    # ... which causes a crash
    with pytest.raises(PreconditionFailed):
        db.write_sync(doc)


def test_revs_limit(db):
    assert db.revs_limit_sync == 1000
    db.revs_limit_sync = 500
    assert db.revs_limit_sync == 500


def test_collate(db):
    docs = [
        Document(None, 1, ('a',), {}),
        Document(False, 1, ('b',), {}),
        Document(True, 1, ('c',), {}),
        Document(-5, 1, ('d',), {}),
        Document(0.5, 1, ('e',), {}),
        Document('abc', 1, ('f',), {}),
        Document('Test', 1, ('g',), {}),
        Document((), 1, ('h',), {}),
        Document((None, 456), 1, ('i',), {}),
        Document((None, '123'), 1, ('j',), {}),
        Document((True, None), 1, ('k',), {}),
        Document(hashabledict({}), 1, ('l',), {}),
        Document(hashabledict({'hello': 'ZZZ'}), 1, ('m',), {}),
        Document(hashabledict({'hello': 'World!', 'more': ''}), 1, ('n',), {}),
    ]
    random.shuffle(docs)  # make things interesting

    with db.write_transaction_sync() as t:
        for doc in docs:
            t.write(doc)
    all_revs_in_collation_order = [doc.path[0] for doc in db.all_docs_sync()]
    assert all_revs_in_collation_order == list('abcdefghijklmn')


@pytest.mark.anyio
async def test_async(db):
    assert await db.update_seq == 0
    # query some unexisting rev
    assert await to_list(db.revs_diff(async_iter([
        ('unexisting', [(1, 'x'), (2, 'y')])
    ]))) == [
        Missing('unexisting', {(1, 'x'), (2, 'y')}, set()),
    ]
    soon_overwritten = Document('mytest', 1, ('x',), {'Hello': 'World!'})
    soon_overwritten.add_attachment('test.csv', async_iter([b'1,2,3']))
    await db.write(soon_overwritten)
    assert len(await to_list(db.all_docs())) == 1
    await db.write(Document('mytest', 2, ('y', 'x',), is_deleted=True))
    with pytest.raises(AttributeError):
        await db.write({})
    req = [
        # three different ways...
        ('mytest', {'revs': 'all'}),
        ('mytest', {}),
        ('mytest', {'revs': [(2, 'y')]}),
    ]
    for id, opts in req:
        assert await to_list(db.read(id, **opts)) == [
            Document('mytest', 2, ('y', 'x'), is_deleted=True),
        ]
    assert await to_list(db.all_docs()) == []
    assert await to_list(db.changes()) == [
        Change('mytest', seq=2, deleted=True, leaf_revs=[(2, 'y')])
    ]
    # try never-existing doc
    with pytest.raises(NotFound):
        await to_list(db.read('abc'))

    assert 'memory' in await db.id

    assert await db.revs_limit == 1000
    await db.set_revs_limit(500)
    assert await db.revs_limit == 500
    assert db.revs_limit_sync == 500

    # some more attachment testing
    new_doc = Document('csv', 1, ('a',), {})
    new_doc.add_attachment('test.csv', async_iter([b'4,5,6']))
    await db.write(new_doc)
    async with db.read_with_attachments('csv', atts_since=[]) as resp:
        loaded_doc = await anext(resp)
        assert await to_list(loaded_doc.attachments['test.csv']) == [
            b'4,5,6'
        ]
    stub_doc = await anext(db.read('csv'))
    assert stub_doc.attachments['test.csv'].is_stub
    # make sure re-inserting stub revision succeeds
    stub_doc.rev_num += 1
    stub_doc.path = ('b',) + stub_doc.path
    await db.write(stub_doc)
