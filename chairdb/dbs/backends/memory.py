import anyio
import sortedcontainers

import contextlib
import copy
import uuid

from ...errors import NotFound
from ...utils import as_future_result


class InMemoryBackend:
    """In-memory storage. Just implements read/write transactions for local
    docs and rev trees. A normal CouchDB API is build on top of that in
    abstract.py

    """
    def __init__(self, id=None):
        self.id = (id or uuid.uuid4().hex) + 'memory'
        self._update_seq = 0

        # id -> document (dict)
        self._local = sortedcontainers.SortedDict()
        # id -> (rev_tree, last_update_seq)
        self._byid = sortedcontainers.SortedDict()
        # seq -> id (str)
        self._byseq = sortedcontainers.SortedDict()

    @contextlib.asynccontextmanager
    async def read_transaction(self):
        yield ReadTransaction(self._local, self._byid, self._byseq,
                              self._update_seq)

    @contextlib.asynccontextmanager
    async def write_transaction(self):
        if not hasattr(self, '_write_lock'):
            self._write_lock = anyio.Lock()
        async with self._write_lock:
            # replace indices with copies such that current readers keep access
            # to the 'old' state
            t = WriteTransaction(self._local.copy(), self._byid.copy(),
                                 self._byseq.copy(), self._update_seq)
            yield t
            # overwrite indices
            self._byid = t._byid
            self._byseq = t._byseq
            self._local = t._local
            self._update_seq = t._update_seq


async def _read(self, id):
    try:
        # find it using the 'by id' index
        tree, _ = self._byid[id]
    except KeyError as e:
        raise NotFound(id) from e
    return tree


async def _read_local(self, id):
    return copy.deepcopy(self._local.get(id))  # allow modification


class ReadTransaction:
    def __init__(self, local, byid, byseq, update_seq):
        self._local = local
        self._byid = byid
        self._byseq = byseq
        self._update_seq = update_seq

    @property
    def update_seq(self):
        return as_future_result(self._update_seq)

    async def all_docs(self, start_key, end_key, descending):
        iter = self._byid.irange(start_key, end_key, reverse=descending)
        for id in iter:
            tree, _ = self._byid[id]
            yield id, tree

    async def all_local_docs(self, start_key, end_key, descending):
        iter = self._local.irange(start_key, end_key, reverse=descending)
        for id in iter:
            yield id, self._local[id]

    async def changes(self, since):
        """If we ever support style='main_only' then storing winner metadata in
        the byseq index would make sense. Now, not so much. We need to query
        the by_id index for the revision tree anyway...

        """
        for seq in self._byseq.irange(minimum=since, inclusive=(False, False)):
            id = self._byseq[seq]
            rev_tree, _ = self._byid[id]

            yield seq, id, rev_tree

    read = _read
    read_local = _read_local


class WriteTransaction:
    def __init__(self, local, byid, byseq, update_seq):
        self._local = local
        self._byid = byid
        self._byseq = byseq
        self._update_seq = update_seq

    read = _read
    read_local = _read_local

    def write(self, id, tree):
        self._update_seq += 1
        with contextlib.suppress(KeyError):
            _, last_update_seq = self._byid[id]
            del self._byseq[last_update_seq]
        # update the by seq index by first removing a previous reference to the
        # current document (if there is one), and then inserting a new one.
        self._byseq[self._update_seq] = id
        # actual insertion by updating the document info in the indices
        self._byid[id] = tree, self._update_seq

    def write_local(self, id, doc):
        if doc is None:
            self._local.pop(id, None)  # silence KeyError
        else:
            self._local[id] = doc
