import sortedcontainers

import uuid

from .datatypes import NotFound, Document
from .revtree import RevisionTree
from .shared import build_change, revs_diff, read_docs, AsyncDatabaseMixin


class InMemoryDatabase(AsyncDatabaseMixin):
    """For documentation, see the InMemoryDatabase class."""

    def __init__(self, id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.id_sync = (id or uuid.uuid4().hex) + 'memory'
        self.update_seq_sync = 0
        self.revs_limit_sync = 1000

        # id -> document (dict)
        self._local = sortedcontainers.SortedDict()
        # id -> (rev_tree, last_update_seq)
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
            rev_tree, _ = self._byid[id]
            yield build_change(id, seq, rev_tree)

    def revs_diff_sync(self, id, revs):
        try:
            rev_tree, _ = self._byid[id]
        except KeyError:
            rev_tree = RevisionTree([])
        return revs_diff(id, revs, rev_tree)

    def write_local_sync(self, id, doc):
        if doc is None:
            self._local.pop(id, None)  # silence KeyError
        else:
            self._local[id] = doc

    def write_sync(self, doc):
        try:
            tree, last_update_seq = self._byid[doc.id]
        except KeyError:
            tree = RevisionTree([])
        else:
            # update the by seq index by first removing a previous reference to
            # the current document (if there is one), and then (later)
            # inserting a new one.
            del self._byseq[last_update_seq]

        tree.merge_with_path(doc.rev_num, doc.path, doc.body,
                             self.revs_limit_sync)
        self.update_seq_sync += 1
        # actual insertion by updating the document info in the indices
        self._byid[doc.id] = tree, self.update_seq_sync
        self._byseq[self.update_seq_sync] = doc.id

        # Let superclass(es) know stuff changed
        self._updated()

    def read_local_sync(self, id):
        return self._local.get(id)

    def read_sync(self, id, revs):
        try:
            # find it using the 'by id' index
            rev_tree, _ = self._byid[id]
            for branch in read_docs(id, revs, rev_tree):
                yield Document(id, *branch)
        except KeyError as e:
            raise NotFound(id) from e
