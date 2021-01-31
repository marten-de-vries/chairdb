import contextlib
import json

from .shared import (ContinuousChangesMixin, revs_diff, as_future_result,
                     build_change, read_docs)
from .revtree import RevisionTree, Branch
from .attachments import AttachmentStore, AttachmentRecord
from .datatypes import Document, AttachmentMetadata
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
    """CREATE TABLE attachment_chunks (
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

WRITE_DOC = "INSERT INTO documents VALUES (NULL, :body, :attachments)"
DELETE_DOC = "DELETE FROM documents WHERE id=:id"
READ_DOC = "SELECT body, attachments FROM documents WHERE id=:id"
# default value of '{}'
STORE_OR_NEW = """SELECT IFNULL(max(attachments), '{}')
FROM documents WHERE id=:id"""

WRITE_CHUNK = "INSERT INTO attachment_chunks VALES (NULL, :data)"
DELETE_CHUNK = "REMOVE FROM attachment_chunks WHERE id=:id"
READ_CHUNK = "SELECT data FROM attachment_chunks WHERE id=:id"

READ_LOCAL = "SELECT document FROM local_documents WHERE id=:id"

READ = "SELECT rev_tree FROM revision_trees WHERE id=:id"


class SQLAttachment:
    def __init__(self, db, att):
        self._db = db
        self._data_ptr = att.data_ptr

        self.meta = att.meta
        self.is_stub = False

    async def __aiter__(self):
        for chunk_ptr in self._data_ptr:
            yield await self._db.fetch_one(READ_CHUNK, {'id': chunk_ptr})[0]


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
        return RevisionTree(Branch(rn, tuple(path), ptr)
                            for rn, path, ptr in json.loads(data))

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
        full_path, old_ptr, old_i = tree.merge_with_path(doc.rev_num, doc.path)
        if not full_path:
            return
        # create new ptr
        doc_ptr = await self._create_doc_ptr(doc, old_ptr)
        # clean up the old one
        await self._db.execute(DELETE_DOC, {'id': old_ptr})
        # then update the rev tree
        tree.update(doc.rev_num, full_path, doc_ptr, old_i)
        values = {'id': doc.id, 'rev_tree': as_json(tree)}
        await self._db.execute(WRITE, values)

        self._updated()

    async def _create_doc_ptr(self, doc, old_ptr):
        doc_ptr = None
        if not doc.is_deleted:
            att_store = await self._att_store(old_ptr)
            old, new = att_store.merge(doc.attachments)
            for data_ptr in old:
                await self._db.executemany(DELETE_CHUNK, [{'id': id}
                                                          for id in data_ptr])
            for name, att in new:
                # TODO: run in parallel?
                data_ptr = []
                async for chunk in att:
                    values = {'data': chunk}
                    chunk_ptr = await self._db.execute(WRITE_CHUNK, values)
                    data_ptr.append(chunk_ptr)
                att_store.add(name, att.metadata, data_ptr)

            values = {'body': as_json(doc.body),
                      'attachments': as_json(att_store)}
            doc_ptr = await self._db.execute(WRITE_DOC, values)
        return doc_ptr

    async def _att_store(self, ptr):
        store = await self._db.fetch_one(STORE_OR_NEW, values={'id': ptr})
        return self._decode_att_store(store[0])

    def _decode_att_store(self, store):
        result = AttachmentStore()
        for name, rec in json.loads(store).items():
            result[name] = AttachmentRecord(AttachmentMetadata(rec[0]), rec[1])
        return result

    async def read_local(self, id):
        with contextlib.suppress(TypeError):
            raw = await self._db.fetch_one(READ_LOCAL, values={'id': id})
            return json.loads(raw[0])

    async def read(self, requested):
        async for args in requested:
            async for doc in self._read_one(*args):
                yield doc

    async def _read_one(self, id, revs, att_names=None, atts_since=None):
        raw_tree = await self._db.fetch_one(READ, {'id': id})
        rev_tree = self._decode_tree(raw_tree[0])
        for branch in read_docs(id, revs, rev_tree):
            values = {'id': branch.leaf_doc_ptr}
            try:
                body_raw, att_raw = await self._db.fetch_one(READ_DOC, values)
            except TypeError:
                body, atts = None, None
            else:
                body = json.loads(body_raw)
                store = self._decode_att_store(att_raw)
                atts, todo = store.read(branch, att_names, atts_since)
                for name, att in todo:
                    atts[name] = SQLAttachment(self._db, att)
            yield Document(id, branch.leaf_rev_num, branch.path, body, atts)

    async def ensure_full_commit(self):
        # no-op, all writes are immediately committed
        pass
