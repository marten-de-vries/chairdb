import asyncio
import json

from .shared import (ContinuousChangesMixin, revs_diff, as_future_result,
                     build_change, update_doc, read_docs)
from .revtree import RevisionTree, Branch
from .datatypes import NotFound, Document
from ..utils import as_json

TABLE_CREATE = [
    """CREATE TABLE documents (
        seq INTEGER PRIMARY KEY,
        id STRING,
        rev_tree JSON,
        winning_branch_idx INTEGER
    )""",
    "CREATE UNIQUE INDEX idx_id ON documents (id)",
    """CREATE TABLE local_documents (
        id STRING PRIMARY KEY,
        document JSON
    )""",
]

UPDATE_SEQ = "SELECT max(seq) AS update_seq from documents"

CHANGES = """SELECT seq, id, rev_tree, winning_branch_idx FROM documents
WHERE seq > :since
ORDER BY seq"""

TREE_ONLY = "SELECT rev_tree FROM documents WHERE id=:id"

WRITE_LOCAL = "INSERT INTO local_documents VALUES (:id, :document)"

WRITE = """INSERT OR REPLACE INTO documents
VALUES (NULL, :id, :rev_tree, :winner)"""

READ_LOCAL = "SELECT document FROM local_documents WHERE id=:id"

READ = "SELECT rev_tree, winning_branch_idx FROM documents WHERE id=:id"


# TODO: serialize/deserialize json + error handling
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
        for seq, id, tree, winning_branch_idx in rows:
            rev_tree = self._decode_tree(tree)
            yield build_change(id, seq, rev_tree, winning_branch_idx)

    async def revs_diff(self, remote):
        async for id, revs in remote:
            yield revs_diff(id, revs, await self._revs_tree(id))

    async def _revs_tree(self, id):
        tree = await self._db.fetch_one(query=TREE_ONLY, values={'id': id})
        if tree:
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
        tree = await self._revs_tree(doc.id)
        # TODO: non-fixed revs limit
        new_tree, winner = update_doc(doc, tree, revs_limit=1000)
        values = {'id': doc.id, 'rev_tree': as_json(new_tree),
                  'winner': winner}
        await self._db.execute(WRITE, values)
        self._update_event.set()
        self._update_event.clear()

    async def read(self, requested):
        async for id, revs in requested:
            if revs:
                values = {'id': id}
                tree, winner = await self._db.fetch_one(READ, values)
                rev_tree = self._decode_tree(tree)
                for doc in read_docs(id, revs, rev_tree, winner):
                    yield doc
            else:
                base = await self._db.fetch_one(query=READ_LOCAL,
                                                values={'id': id})
                if base:
                    yield Document(id, body=json.loads(base[0]))
                else:
                    yield NotFound(id)

    async def ensure_full_commit(self):
        # no-op, all writes are immediately committed
        pass
