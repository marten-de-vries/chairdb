import sortedcontainers

import asyncio
import uuid
import typing

from .datatypes import NotFound
from .revtree import RevisionTree
from .shared import (build_change, revs_diff, prepare_doc_write, read_docs,
                     ContinuousChangesMixin, to_local_doc, is_local,
                     update_doc, as_future_result)


class DocumentInfo(typing.NamedTuple):
    """An internal representation used as value in the 'by id' index."""

    rev_tree: RevisionTree
    winning_leaf_idx: int
    last_update_seq: int


class SyncInMemoryDatabase:
    """For documentation, see the InMemoryDatabase class."""

    def __init__(self, id=None):
        self.id_sync = (id or uuid.uuid4().hex) + 'memory'
        self.update_seq_sync = 0
        self.revs_limit_sync = 1000

        # id -> document (dict)
        self._local = sortedcontainers.SortedDict()
        # id -> DocumentInfo
        self._byid = sortedcontainers.SortedDict()
        # seq -> id (str)
        self._byseq = sortedcontainers.SortedDict()

    def changes_sync(self, since=None):
        """If we ever support style='main_only' then storing winner metadata in
        the byseq index would make sense. Now, not so much. We need to query
        the by_id index for the revision tree anyway...

        """
        for seq in self._byseq.irange(minimum=since, inclusive=(False, False)):
            id = self._byseq[seq]
            rev_tree, winner, _ = self._byid[id]
            yield build_change(id, seq, rev_tree, winner)

    def revs_diff_sync(self, id, revs):
        try:
            rev_tree = self._byid[id].rev_tree
        except KeyError:
            rev_tree = None
        return revs_diff(id, revs, rev_tree)

    def write_sync(self, doc):
        id, revs, doc = prepare_doc_write(doc)
        if not revs:
            self._write_local(id, doc)
        else:
            self._write_normal(id, revs, doc)

    def _write_local(self, id, doc):
        if not doc:
            self._local.pop(id, None)  # silence KeyError
        else:
            self._local[id] = doc

    def _write_normal(self, id, revs, doc):
        try:
            tree, _, last_update_seq = self._byid[id]
        except KeyError:
            tree = None
        else:
            # update the by seq index by first removing a previous reference to
            # the current document (if there is one), and then (later)
            # inserting a new one.
            del self._byseq[last_update_seq]

        new_doc_info = update_doc(id, revs, doc, tree, self.revs_limit_sync)
        self.update_seq_sync += 1
        # actual insertion by updating the document info in the 'by id' index.
        self._byid[id] = DocumentInfo(*new_doc_info, self.update_seq_sync)
        self._byseq[self.update_seq_sync] = id

    def read_sync(self, id, revs, include_path=False):
        try:
            if is_local(id):
                # load from the _local key-value store
                yield to_local_doc(id, revs, self._local[id])
            else:
                # find it using the 'by id' index
                rev_tree, winner, _ = self._byid[id]
                yield from read_docs(id, revs, include_path, rev_tree, winner)
        except KeyError as e:
            raise NotFound(id) from e


class InMemoryDatabase(SyncInMemoryDatabase, ContinuousChangesMixin):
    """A minimal in-memory implementation of a CouchDB-compatible database.

    The database does not keep the documents for non-leaf revisions for
    simplicity, which has the nice side-effect of effectively auto-compacting
    the database continously. This means you cannot use revisions as a history
    mechanism, though. (Which isn't recommended anyway.)

    Attachments, views and purging are not implemented, but everything
    essential for replication is there.

    Note that writing acts similar to _bulk_docs with new_edits=false. So you
    need to manually generate new revisions (and check for conflicts, I guess).

    An in-memory database is (obviously) implemented synchronously. But such
    an interface does not make sense for databases that have to be reached
    through the network, or arguably even for on-disk databases. To be
    compatible with those, we wrap the (synchronous) in-memory database with
    an asynchronous API. Note that because this class inherits from
    SyncInMemoryDatabase, you can still use the synchronous API. We recommend
    you do so if at all possible as the asynchronous methods just add overhead.

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._update_event = asyncio.Event()

    def _write_normal(self, *args, **kwargs):
        super()._write_normal(*args, **kwargs)

        self._update_event.set()
        self._update_event = asyncio.Event()

    @property
    def id(self):
        """For identification of this specific database during replication. For
        a (volatile) in-memory database, a random uuid (i.e. the default) is
        actually quite a reasonable choice.

        """
        return as_future_result(self.id_sync)

    @property
    def update_seq(self):
        """Each database modification increases this. Starting at zero by
        convention.

        """
        return as_future_result(self.update_seq_sync)

    async def _changes(self, since):
        for change in self.changes_sync(since):
            yield change

    async def revs_diff(self, remote):
        """Like CouchDB's _revs_diff"""

        async for id, revs in remote:
            yield self.revs_diff_sync(id, revs)

    async def write(self, docs):
        """Like CouchDB's _bulk_docs with new_edits=false"""

        async for doc in docs:
            try:
                self.write_sync(doc)
            except (AssertionError, KeyError) as exc:
                yield exc

    async def read(self, requested, include_path=False):
        """Like CouchDB's GET dbname/docid?latest=true, but allows asking for
        multiple documents at once. 'reqested' is an (async) iterable of
        (id, revs) tuples. 'id' is the document id. 'revs' specify which
        version(s) of said document you want to access. 'revs' can be:

        - 'winner' (what you would get by default from CouchDB)
        - 'all' (what you would get from CouchDB by include 'open_revs=all',
          i.e. all leafs)
        - a list of revisions (which you would get from CouchDB when manually
          specifying 'open_revs=[...]'.

        include_path=True is like setting 'revs=true' on CouchDB, i.e. it
        includes a '_revisions' key in the document.

        Note that, for non-'winner' values of revs, this can return multiple
        document leafs. That's why this method is an (async) generator.

        """
        async for id, revs in requested:
            try:
                for doc in self.read_sync(id, revs):
                    yield doc
            except NotFound as exc:
                yield exc

    async def ensure_full_commit(self):
        pass  # no-op for an in-memory db

    @property
    def revs_limit(self):
        return as_future_result(self.revs_limit_sync)

    async def set_revs_limit(self, value):
        self.revs_limit_sync = value
