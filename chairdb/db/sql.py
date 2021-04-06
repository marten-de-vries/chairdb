import anyio
import contextlib
import json

from .shared import (ContinuousChangesMixin, revs_diff, as_future_result,
                     build_change, read_docs, BasicWriteTransaction,
                     TransactionBasedDBMixin)
from .revtree import RevisionTree, Branch
from .attachments import AttachmentStore, AttachmentRecord
from .datatypes import Document, AttachmentMetadata, NotFound
from ..utils import as_json

TABLE_CREATE = [
    """CREATE TABLE revision_trees (
        id STRING PRIMARY KEY,
        rev_tree JSON,
        seq INTEGER
    ) WITHOUT ROWID""",
    "CREATE UNIQUE INDEX idx_seq ON revision_trees (seq)",
    """CREATE TABLE documents (
        id INTEGER PRIMARY KEY,
        body JSON,
        attachments JSON
    )""",
    """CREATE TABLE attachment_chunks (
        id INTEGER PRIMARY KEY,
        data BLOB
    )""",
    """CREATE TABLE local_documents (
        id STRING PRIMARY KEY,
        document JSON
    ) WITHOUT ROWID""",
]

UPDATE_SEQ = "SELECT COALESCE(MAX(seq), 0) AS update_seq from revision_trees"

CHANGES = """SELECT seq, id, rev_tree FROM revision_trees
WHERE seq > :since
ORDER BY seq"""

# default value of '[]'
TREE_OR_NEW = """SELECT COALESCE(MAX(rev_tree), '[]')
FROM revision_trees WHERE id=:id"""

WRITE_LOCAL = "INSERT OR REPLACE INTO local_documents VALUES (:id, :document)"

WRITE = f"""INSERT OR REPLACE INTO revision_trees VALUES (:id, :rev_tree,
    ({UPDATE_SEQ}) + 1
)"""

WRITE_DOC = "INSERT INTO documents VALUES (NULL, :body, :attachments)"
DELETE_DOC = "DELETE FROM documents WHERE id=:id"
READ_DOC = "SELECT body, attachments FROM documents WHERE id=:id"
# default value of '{}'
STORE_OR_NEW = """SELECT COALESCE(MAX(attachments), '{}')
FROM documents WHERE id=:id"""

WRITE_CHUNK = "INSERT INTO attachment_chunks VALUES (NULL, :data)"
DELETE_CHUNK = "REMOVE FROM attachment_chunks WHERE id=:id"
READ_CHUNK = "SELECT data FROM attachment_chunks WHERE id=:id"

READ_LOCAL = "SELECT document FROM local_documents WHERE id=:id"

READ = "SELECT rev_tree FROM revision_trees WHERE id=:id"


class SQLAttachment:
    def __init__(self, tx, att):
        self._tx = tx
        self._data_ptr = att.data_ptr

        self.meta = att.meta
        self.is_stub = False

    async def __aiter__(self):
        for chunk_ptr in self._data_ptr:
            values = {'id': chunk_ptr}
            async with self._tx.execute(READ_CHUNK, values) as cursor:
                chunk = (await cursor.fetchone())[0]
            yield chunk


class SQLDatabase(ContinuousChangesMixin, TransactionBasedDBMixin):
    def __init__(self, pool, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pool = pool

    async def __aenter__(self):
        async with self._pool.write_transaction() as conn:
            for query in TABLE_CREATE:
                await conn.execute(query)
        return self

    async def __aexit__(self, *_):
        """No-op for now."""

    @contextlib.asynccontextmanager
    async def write_transaction(self):
        actions = []
        yield WriteTransaction(actions)
        async with self._pool.write_transaction() as tx:
            for action, *args in actions:
                await ({
                    'write': self._write_impl,
                    'write_local': self._write_local_impl,
                    # TODO: revs_limit
                }[action])(tx, *args)

    @contextlib.asynccontextmanager
    async def read_transaction(self):
        async with self._pool.read_transaction() as tx:
            yield ReadTransaction(tx)

    @property
    def id(self):
        return as_future_result(str(self._pool.url) + 'sql')

    async def _write_local_impl(self, tx, id, doc):
        values = {'id': id, 'document': as_json(doc)}
        await tx.execute(WRITE_LOCAL, values)

    async def _write_impl(self, tx, doc):
        tree = await _revs_tree(tx, doc.id)
        full_path, old_ptr, old_i = tree.merge_with_path(doc.rev_num, doc.path)
        if not full_path:
            return
        # create new ptr
        doc_ptr = await self._create_doc_ptr(tx, doc, old_ptr)
        # clean up the old one
        await tx.execute(DELETE_DOC, {'id': old_ptr})
        # then update the rev tree
        tree.update(doc.rev_num, full_path, doc_ptr, old_i)
        values = {'id': doc.id, 'rev_tree': as_json(tree)}
        await tx.execute(WRITE, values)

        self._updated()

    async def _create_doc_ptr(self, tx, doc, old_ptr):
        doc_ptr = None
        if not doc.is_deleted:
            att_store = await self._att_store(tx, old_ptr)
            old, new = att_store.merge(doc.attachments)
            for data_ptr in old:
                await tx.executemany(DELETE_CHUNK, [{'id': id}
                                                    for id in data_ptr])
            async with anyio.create_task_group() as tg:
                for name, att in new:
                    tg.start_soon(self._read_att, tx, name, att, att_store)

            values = {'body': as_json(doc.body),
                      'attachments': as_json(att_store)}
            async with tx.execute(WRITE_DOC, values) as cursor:
                doc_ptr = cursor.lastrowid
        return doc_ptr

    async def _read_att(self, tx, name, att, att_store):
        data_ptr = []
        async for chunk in att:
            values = {'data': chunk}
            async with await tx.execute(WRITE_CHUNK, values) as cursor:
                chunk_ptr = cursor.lastrowid
            data_ptr.append(chunk_ptr)
        att_store.add(name, att.meta, data_ptr)

    async def _att_store(self, tx, ptr):
        async with await tx.execute(STORE_OR_NEW, {'id': ptr}) as cursor:
            store = await cursor.fetchone()
        return _decode_att_store(store[0])

    async def ensure_full_commit(self):
        # no-op, all writes are immediately committed
        pass


class ReadTransaction:
    def __init__(self, tx):
        self._tx = tx

    @property
    def update_seq(self):
        return self._get_update_seq()

    async def _get_update_seq(self):
        async with self._tx.execute(UPDATE_SEQ) as cursor:
            return (await cursor.fetchone())[0]

    async def changes(self, since=None):
        async with self._tx.execute(CHANGES, {'since': since or 0}) as cursor:
            while True:
                rows = await cursor.fetchmany()
                if not rows:
                    break
                for seq, id, tree in rows:
                    rev_tree = _decode_tree(tree)
                    yield build_change(id, seq, rev_tree)

    async def revs_diff(self, id, revs):
        return revs_diff(id, revs, await _revs_tree(self._tx, id))

    async def read_local(self, id):
        with contextlib.suppress(TypeError):
            async with self._tx.execute(READ_LOCAL, {'id': id}) as cursor:
                raw = await cursor.fetchone()
            return json.loads(raw[0])

    async def read(self, id, **opts):
        async with self._tx.execute(READ, {'id': id}) as cursor:
            raw_tree = await cursor.fetchone()
        try:
            rev_tree = _decode_tree(raw_tree[0])
        except TypeError:
            yield NotFound(id)
        else:
            async for doc in self._read_one(rev_tree, id, **opts):
                yield doc

    async def _read_one(self, tree, id, revs=None, att_names=None,
                        atts_since=None):
        for branch in read_docs(id, revs, tree):
            values = {'id': branch.leaf_doc_ptr}
            try:
                async with self._tx.execute(READ_DOC, values) as cursor:
                    body_raw, att_raw = await cursor.fetchone()
            except TypeError:
                body, atts = None, None
            else:
                body = json.loads(body_raw)
                store = _decode_att_store(att_raw)
                atts, todo = store.read(branch, att_names, atts_since)
                for name, att in todo:
                    atts[name] = SQLAttachment(self._tx, att)
            yield Document(id, branch.leaf_rev_num, branch.path, body, atts)


# helpers
async def _revs_tree(db, id):
    async with db.execute(TREE_OR_NEW, {'id': id}) as cursor:
        tree = await cursor.fetchone()
    return _decode_tree(tree[0])


def _decode_tree(data):
    return RevisionTree(Branch(rn, tuple(path), ptr)
                        for rn, path, ptr in json.loads(data))


def _decode_att_store(store):
    result = AttachmentStore()
    for name, (meta, ptr) in json.loads(store).items():
        result[name] = AttachmentRecord(AttachmentMetadata(*meta), ptr)
    return result


class WriteTransaction(BasicWriteTransaction):
    # TODO: make sure the attachments have been written to shorten the
    # transaction
    pass
