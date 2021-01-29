import contextlib
import json

from .shared import (ContinuousChangesMixin, revs_diff, as_future_result,
                     build_change, read_docs)
from .revtree import RevisionTree, Branch
from .datatypes import Document
from ..utils import as_json

TABLE_CREATE = [
    """CREATE TABLE revision_trees (
        seq INTEGER PRIMARY KEY,
        id STRING,
        rev_tree JSON
    )""",
    """CREATE TABLE documents (
        id INTEGER PRIMARY KEY,
        body JSON,
        attachments JSON
    )""",
    """CREATE TABLE attachments (
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

WRITE_DOC = "INSERT INTO documents VALUES (NULL, :body, NULL)"
DELETE_DOC = "DELETE FROM documents WHERE id=:id"
READ_DOC = "SELECT body FROM documents WHERE id=:id"

READ_LOCAL = "SELECT document FROM local_documents WHERE id=:id"

READ = "SELECT rev_tree FROM revision_trees WHERE id=:id"


# TODO: error handling
class SQLDatabase(ContinuousChangesMixin):
    def __init__(self, db, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db = db

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
            try:
                await self._write_doc(doc)
            except Exception as exc:
                yield exc

    async def write_local(self, id, doc):
        values = {'id': id, 'document': as_json(doc)}
        await self._db.execute(WRITE_LOCAL, values)

    async def _write_doc(self, doc):
        tree = await self._revs_tree(doc.id)
        full_path, old_index = tree.merge_with_path(doc.rev_num, doc.path)
        if not full_path:
            return
        if old_index is not None:
            old_doc_ptr = tree[old_index].leaf_doc_ptr
            await self._db.execute(DELETE_DOC, {'id': old_doc_ptr})
        doc_ptr = await self._db.execute(WRITE_DOC, value={'doc': doc.body})
        # TODO: revs limit
        tree.update(doc.rev_num, full_path, doc_ptr, old_index)
        values = {'id': doc.id, 'rev_tree': as_json(tree)}
        await self._db.execute(WRITE, values)

        self._updated()

    async def read_local(self, id):
        with contextlib.suppress(TypeError):
            return await self._fetch_body(READ_LOCAL, id)

    async def read(self, requested):
        async for id, revs in requested:
            raw_tree = await self._db.fetch_one(READ, {'id': id})
            rev_tree = self._decode_tree(raw_tree[0])
            for branch in read_docs(id, revs, rev_tree):
                body = branch.leaf_doc_ptr
                if body is not None:
                    body = await self._fetch_body(READ_DOC, body)
                yield Document(id, branch.leaf_rev_num, branch.path, body)

    async def _fetch_body(self, query, id):
        raw_result = await self._db.fetch_one(query, values={'id': id})
        return json.loads(raw_result[0])

    async def ensure_full_commit(self):
        # no-op, all writes are immediately committed
        pass
