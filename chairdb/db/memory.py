import sortedcontainers

import uuid

from .datatypes import NotFound, Document
from .attachments import AttachmentStore
from .revtree import RevisionTree
from .shared import build_change, revs_diff, read_docs, AsyncDatabaseMixin
from ..utils import InMemoryAttachment


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
            rev_tree = RevisionTree()
        return revs_diff(id, revs, rev_tree)

    def write_local_sync(self, id, doc):
        if doc is None:
            self._local.pop(id, None)  # silence KeyError
        else:
            self._local[id] = doc

    def write_sync(self, doc):
        # get the new document's path and check if it replaces something
        try:
            tree, last_update_seq = self._byid[doc.id]
        except KeyError:
            tree, last_update_seq = RevisionTree(), None

        full_path, old_index = tree.merge_with_path(doc.rev_num, doc.path)
        if not full_path:
            return  # document already in the database.

        doc_ptr = self._create_doc_ptr(doc, tree, old_index)
        # insert or replace in the rev tree
        tree.update(doc.rev_num, full_path, doc_ptr, old_index,
                    self.revs_limit_sync)

        self.update_seq_sync += 1
        # actual insertion by updating the document info in the indices
        self._byid[doc.id] = tree, self.update_seq_sync
        # update the by seq index by first removing a previous reference to the
        # current document (if there is one), and then inserting a new one.
        if last_update_seq:
            del self._byseq[last_update_seq]
        self._byseq[self.update_seq_sync] = doc.id

        # Let superclass(es) know stuff changed
        self._updated()

    def _create_doc_ptr(self, doc, tree, old_index):
        """A doc_ptr is a (body, attachments) tuple for the in-memory case."""

        doc_ptr = None
        if not doc.is_deleted:
            try:
                _, att_store = tree[old_index].leaf_doc_ptr
            except TypeError:
                att_store = AttachmentStore()
            todo = att_store.merge(doc.attachments)
            for name, attachment in todo:
                data_ptr = b''.join(attachment)
                att_store.add(name, attachment.meta, data_ptr)
            doc_ptr = (doc.body, att_store)
        return doc_ptr

    def read_local_sync(self, id):
        return self._local.get(id)

    def read_sync(self, id, revs, att_names=None, atts_since=None):
        try:
            # find it using the 'by id' index
            rev_tree, _ = self._byid[id]
        except KeyError as e:
            raise NotFound(id) from e

        for branch in read_docs(id, revs, rev_tree):
            yield from self._read_doc(id, branch, att_names, atts_since)

    def _read_doc(self, id, branch, att_names, atts_since):
        rev_num = branch.leaf_rev_num
        try:
            doc, att_store = branch.leaf_doc_ptr
        except TypeError:
            doc, atts = None, None
        else:
            atts, todo = att_store.read(branch, att_names, atts_since)
            for name, info in todo:
                atts[name] = InMemoryAttachment(info.meta, info.data_ptr)
        yield Document(id, rev_num, branch.path, doc, atts)
