import sortedcontainers

import asyncio
import typing
import uuid

from .revtree import Leaf, RevisionTree, validate_rev_tree
from .utils import rev, parse_rev
from .datatypes import NotFound, Change


class DocumentInfo(typing.NamedTuple):
    """An internal representation used as value in the 'by id' index."""

    winner_rev_num: int
    winner_leaf: Leaf
    rev_tree: RevisionTree
    last_update_seq: int


class SyncInMemoryDatabase:
    """For documentation, see the InMemoryDatabase class."""

    def __init__(self, id=None):
        self.id_sync = (id or uuid.uuid4().hex) + 'memory'
        self.update_seq_sync = 0

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
            doc_info = self._byid[id]
            deleted = doc_info.winner_leaf.doc_ptr is None
            leaf_revs = [rev(rev_num, leaf)
                         for rev_num, _, leaf in doc_info.rev_tree.leafs()]
            yield Change(id, seq, deleted, leaf_revs)

    def revs_diff_sync(self, id, revs):
        try:
            doc_info = self._byid[id]
        except KeyError:
            missing = set(revs)
        else:
            missing = set(revs).difference(doc_info.rev_tree.all_revs())
        return id, {'missing': missing}

    def write_sync(self, doc):
        if doc['_id'].startswith('_local/'):
            self._write_local(doc)
        else:
            self._write_normal(doc)

    def _write_local(self, doc):
        if doc.get('_deleted'):
            self._local.pop(doc['_id'], None)  # silence KeyError
        else:
            self._local[doc['_id']] = doc

    def _write_normal(self, doc):
        id = doc.pop('_id')
        # normalize _revisions field & handle delete flag
        revs, doc = self._prepare_doc(doc)

        # actual insertion by updating the document info in the 'by id' index.
        self.update_seq_sync += 1
        rev_tree, old_seq = self._update_rev_tree(id, revs, doc)
        winner_rev_num, winner_leaf = rev_tree.winner()
        self._byid[id] = DocumentInfo(winner_rev_num, winner_leaf,
                                      rev_tree, self.update_seq_sync)

        # update the by seq index by first removing a previous reference to the
        # current document (if there is one), and then inserting a new one.
        if old_seq:
            del self._byseq[old_seq]
        self._byseq[self.update_seq_sync] = id

    def _prepare_doc(self, doc):
        rev_num, rev_hash = parse_rev(doc.pop('_rev'))
        revs = doc.pop('_revisions', {'start': rev_num, 'ids': [rev_hash]})
        assert revs['ids'][0] == rev_hash, 'Invalid _revisions'
        if doc.get('_deleted'):
            doc = None
        return revs, doc

    def _update_rev_tree(self, id, revs, doc):
        try:
            # load existing tree
            _, _, rev_tree, old_seq = self._byid[id]
        except KeyError:
            rev_tree, old_seq = RevisionTree([]), None  # new empty tree

        rev_tree.merge_with_path(revs['start'], revs['ids'], doc)
        validate_rev_tree(rev_tree)  # TODO: remove
        return rev_tree, old_seq

    def read_sync(self, id, revs, include_path=False):
        if id.startswith('_local/'):
            # load from the _local key-value store
            yield self._read_local(id, revs)
        else:
            # find it using the 'by id' index
            yield from self._read_normal(id, revs, include_path)

    def _read_local(self, id, revs):
        assert revs == 'winner'
        try:
            # the revision is fixed
            return {**self._local[id], '_rev': '0-1'}
        except KeyError as e:
            raise NotFound(id) from e

    def _read_normal(self, id, revs, include_path):
        # load document info
        try:
            winning_rev_num, winning_leaf, rev_tree, _ = self._byid[id]
        except KeyError as e:
            raise NotFound(id) from e

        if include_path and revs == 'winner':
            # we need the path in the tree, so we need to start from scratch
            # in finding the winning leaf.
            revs = [rev(winning_rev_num, winning_leaf)]
        if revs == 'winner':
            # the information is stored in the DocumentInfo directly
            yield self._to_doc(id, winning_rev_num, winning_leaf)
        else:
            # ... walk the revision tree
            yield from self._read_revs(id, revs, rev_tree, include_path)

    def _read_revs(self, id, revs, rev_tree, include_path):
        if revs == 'all':
            # all leafs
            for rev_num, path, leaf in rev_tree.leafs(include_path):
                yield self._to_doc(id, rev_num, leaf, path)
        else:
            # search for specific revisions
            for rev_num, path, leaf in rev_tree.find(revs, include_path):
                yield self._to_doc(id, rev_num, leaf, path)

    def _to_doc(self, id, rev_num, leaf, path=None):
        """Reconstruct a CouchDB-compatible JSON document from the gathered
        information

        """
        doc = {'_id': id, '_rev': rev(rev_num, leaf)}
        if leaf.doc_ptr is None:
            doc['_deleted'] = True
        else:
            doc.update(leaf.doc_ptr)
        if path is not None:
            ids = [leaf.rev_hash] + [rev_hash for rev_hash in path]
            doc['_revisions'] = {'start': rev_num, 'ids': ids}
        return doc


class InMemoryDatabase(SyncInMemoryDatabase):
    """A minimal in-memory implementation of a CouchDB-compatible database.

    The database does not keep the documents for non-leaf revisions for
    simplicity, which has the nice side-effect of effectively auto-compacting
    the database continously. This means you cannot use revisions as a history
    mechanism, though. (Which isn't recommended anyway.)

    Attachments, views and purging are not implemented, but everything
    essential for replication is implemented.

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
    @property
    def id(self):
        """For identification of this specific database during replication. For
        a (volatile) in-memory database, a random uuid is actually quite a
        reasonable choice.

        """
        return self._as_future_result(self.id_sync)

    def _as_future_result(self, value):
        future = asyncio.get_event_loop().create_future()
        future.set_result(value)
        return future

    @property
    def update_seq(self):
        """Each database modification increases this. Starting at zero by
        convention.

        """
        return self._as_future_result(self.update_seq_sync)

    async def changes(self, since=None):
        """"Like CouchDB's _changes with style=all_docs"""

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
        pass  # no-op
