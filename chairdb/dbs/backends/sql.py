import contextlib
import functools
import json
import sqlite3

from ..revtree import RevisionTree, Branch
from ...errors import NotFound
from ...utils import as_json

TABLE_CREATE = [
    """CREATE TABLE revision_trees (
        id STRING PRIMARY KEY,
        rev_tree JSON,
        seq INTEGER
    ) WITHOUT ROWID""",
    "CREATE UNIQUE INDEX idx_seq ON revision_trees (seq)",
    """CREATE TABLE local_documents (
        id STRING PRIMARY KEY,
        is_json BOOLEAN,
        data BLOB
    ) WITHOUT ROWID""",
]

UPDATE_SEQ = "SELECT COALESCE(MAX(seq), 0) AS update_seq from revision_trees"


def range_query(base, start_key, end_key, descending):
    parts = [base]
    start_key = start_key is not None
    end_key = end_key is not None

    if start_key or end_key:
        parts.append("WHERE")
    if start_key:
        parts.append("id >= :start_key")
        if end_key:
            parts.append("AND")
    if end_key:
        parts.append("id <= :end_key")
    parts.append(f"ORDER BY id {'DESC' if descending else 'ASC'}")
    return ' '.join(parts)


all_local_docs_base = "SELECT id, is_json, data FROM local_documents"
all_local_docs_query = functools.partial(range_query, all_local_docs_base)
all_docs_base = "SELECT id, rev_tree FROM revision_trees"
all_docs_query = functools.partial(range_query, all_docs_base)


ALL_DOCS = [
    f"""SELECT id, rev_tree FROM revision_trees
    WHERE id BETWEEN :start_key AND :end_key ORDER BY id {order}"""
    for order in ['ASC', 'DESC']
]

CHANGES = """SELECT seq, id, rev_tree FROM revision_trees
WHERE seq > :since
ORDER BY seq"""

WRITE_LOCAL = """INSERT OR REPLACE INTO local_documents
VALUES (:id, :is_json, :data)"""

READ_LOCAL = "SELECT is_json, data FROM local_documents WHERE id=:id"
DELETE_LOCAL = "DELETE FROM local_documents WHERE id=:id"

READ = "SELECT rev_tree FROM revision_trees WHERE id=:id"
WRITE = f"""INSERT OR REPLACE INTO revision_trees VALUES (:id, :rev_tree,
    ({UPDATE_SEQ}) + 1
)"""


class SQLBackend:
    """SQLite storage. Just implements read/write transactions for local docs
    and rev trees.

    """
    def __init__(self, pool):
        self._pool = pool
        self.id = str(self._pool.url) + 'sql'

    async def create(self):
        async with self._pool.write_transaction() as conn:
            for query in TABLE_CREATE:
                try:
                    await conn.execute(query)
                except sqlite3.OperationalError:
                    return False
        return True

    @contextlib.asynccontextmanager
    async def read_transaction(self):
        async with self._pool.read_transaction() as tx:
            yield ReadTransaction(tx)

    @contextlib.asynccontextmanager
    async def write_transaction(self):
        async with self._pool.write_transaction() as tx:
            t = WriteTransaction(tx)
            yield t
            await tx.executemany(WRITE_LOCAL, t.local_writes)
            await tx.executemany(DELETE_LOCAL, t.local_deletes)
            await tx.executemany(WRITE, t.docs)


async def _read(self, id):
    async with self._tx.execute(READ, {'id': id}) as cursor:
        raw_tree = await cursor.fetchone()
    try:
        return decode_tree(raw_tree[0])
    except TypeError as e:
        raise NotFound(id) from e


async def _read_local(self, id):
    async with self._tx.execute(READ_LOCAL, {'id': id}) as cursor:
        with contextlib.suppress(TypeError):
            return decode_local(*await cursor.fetchone())


class ReadTransaction:
    def __init__(self, tx):
        self._tx = tx

    @property
    def update_seq(self):
        return self._get_update_seq()

    async def _get_update_seq(self):
        async with self._tx.execute(UPDATE_SEQ) as cursor:
            return (await cursor.fetchone())[0]

    async def _rows(self, cursor):
        while True:
            rows = await cursor.fetchmany()
            if not rows:
                return
            for row in rows:
                yield row

    async def all_docs(self, start_key, end_key, descending):
        query = all_docs_query(start_key, end_key, descending)
        values = {'start_key': start_key, 'end_key': end_key}
        async with self._tx.execute(query, values) as cursor:
            async for id, tree in self._rows(cursor):
                yield id, decode_tree(tree)

    async def all_local_docs(self, start_key, end_key, descending):
        query = all_local_docs_query(start_key, end_key, descending)
        values = {'start_key': start_key, 'end_key': end_key}
        async with self._tx.execute(query, values) as cursor:
            async for id, *args in self._rows(cursor):
                yield id, decode_local(*args)

    async def changes(self, since=None):
        async with self._tx.execute(CHANGES, {'since': since or 0}) as cursor:
            async for seq, id, tree in self._rows(cursor):
                yield seq, id, decode_tree(tree)

    read = _read
    read_local = _read_local


class WriteTransaction:
    def __init__(self, tx):
        self._tx = tx  # for read/read_local
        self.docs = []
        self.local_writes = []
        self.local_deletes = []

    def write_local(self, id, data):
        result = {'id': id}
        if data is None:
            self.local_deletes.append(result)
        else:
            if isinstance(data, bytes):
                result['is_json'] = False
                result['data'] = data
            else:
                result['is_json'] = True
                result['data'] = as_json(data)
            self.local_writes.append(result)

    def write(self, id, tree):
        self.docs.append({'id': id, 'rev_tree': as_json(tree)})

    read = _read
    read_local = _read_local


def decode_tree(data):
    return RevisionTree(Branch(rn, tuple(path), ptr)
                        for rn, path, ptr in json.loads(data))


def decode_local(is_json, data):
    if is_json:
        return json.loads(data)
    return data
