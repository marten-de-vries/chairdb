import asyncio
import json

from .shared import (ContinuousChangesMixin, revs_diff, as_future_result,
                     build_change, read_docs)
from .revtree import RevisionTree, Branch
from .datatypes import NotFound, Document, LocalDocument
from ..utils import as_json

TABLE_CREATE = [
    """CREATE TABLE revision_trees (
        seq INTEGER PRIMARY KEY,
        id STRING,
        rev_tree JSON
    )""",
    """CREATE TABLE blobs (
        id INTEGER PRIMARY KEY,
        data BLOB
    )""",
    "CREATE UNIQUE INDEX idx_id ON revision_trees (id)",
    """CREATE TABLE local_documents (
        id STRING PRIMARY KEY,
        document JSON
    )""",
]

UPDATE_SEQ = "SELECT max(seq) AS update_seq from revision_trees"

CHANGES = """SELECT seq, id, rev_tree FROM revision_trees
WHERE seq > :since
ORDER BY seq"""

# default value of '[]'
TREE_OR_NEW = """SELECT IFNULL(MAX(rev_tree), '[]')
FROM revision_trees WHERE id=:id"""

WRITE_LOCAL = "INSERT INTO local_documents VALUES (:id, :document)"

WRITE = """INSERT OR REPLACE INTO revision_trees
VALUES (NULL, :id, :rev_tree)"""

WRITE_BLOB = "INSERT INTO blobs VALUES (NULL, :data)"
DELETE_BLOB = "DELETE FROM blobs WHERE id=:id"
READ_BLOB = "SELECT data FROM blobs WHERE id=:id"

READ_LOCAL = "SELECT document FROM local_documents WHERE id=:id"

READ = "SELECT rev_tree FROM revision_trees WHERE id=:id"


# TODO: error handling
class SQLDatabase(ContinuousChangesMixin):
    def __init__(self, db):
        self._db = db
        self._update_event = asyncio.Event()

    async def __aenter__(self):
        for query in TABLE_CREATE:
            await self._db.execute(query=query)
        return self

    async def __aexit__(self, *_):
        """TODO"""

    @property
    def id(self):
        return as_future_result(str(self._db.url) + 'sql')

    @property
    def update_seq(self):
        return self._get_update_seq()

    async def _get_update_seq(self):
        seq, = await self._db.fetch_one(query=UPDATE_SEQ)
        return seq or 0

    async def _changes(self, since=None):
        rows = await self._db.fetch_all(query=CHANGES,
                                        values={'since': since or 0})
        for seq, id, tree in rows:
            rev_tree = self._decode_tree(tree)
            yield build_change(id, seq, rev_tree)

    async def revs_diff(self, remote):
        async for id, revs in remote:
            yield revs_diff(id, revs, await self._revs_tree(id))

    async def _revs_tree(self, id):
        tree = await self._db.fetch_one(query=TREE_OR_NEW, values={'id': id})
        return self._decode_tree(tree[0])

    def _decode_tree(self, data):
        return RevisionTree([Branch(*branch) for branch in json.loads(data)])

    async def write(self, docs):
        async for doc in docs:
            if doc.is_local:
                values = {'id': doc.id, 'document': as_json(doc.body)}
                await self._db.execute(WRITE_LOCAL, values)
            else:
                try:
                    await self._write_doc(doc)
                except Exception as exc:
                    yield exc

    async def _write_doc(self, doc):
        # insert blob
        ptr = doc.body
        if ptr:
            value = {'data': as_json(doc.body)}
            ptr = await self._db.execute(WRITE_BLOB, value)

        tree = await self._revs_tree(doc.id)
        # TODO: non-fixed revs limit
        old_ptr = tree.merge_with_path(doc.rev_num, doc.path, ptr, 1000)
        await self._db.execute(DELETE_BLOB, {'id': old_ptr})
        values = {'id': doc.id, 'rev_tree': as_json(tree)}
        await self._db.execute(WRITE, values)
        self._update_event.set()
        self._update_event.clear()

    async def read(self, requested):
        async for id, revs in requested:
            if revs == 'local':
                yield await self._read_local(id)
            else:
                raw_tree = await self._db.fetch_one(READ, {'id': id})
                rev_tree = self._decode_tree(raw_tree[0])
                for branch in read_docs(id, revs, rev_tree):
                    body = branch.leaf_doc_ptr
                    if body is not None:
                        body = await self._fetch_body(READ_BLOB, body)
                    yield Document(id, branch.leaf_rev_num, branch.path, body)

    async def _read_local(self, id):
        try:
            body = await self._fetch_body(READ_LOCAL, id)
            return LocalDocument(id, body)
        except TypeError:
            return NotFound(id)

    async def _fetch_body(self, query, id):
        raw_result = await self._db.fetch_one(query, values={'id': id})
        return json.loads(raw_result[0])

    async def ensure_full_commit(self):
        # no-op, all writes are immediately committed
        pass
