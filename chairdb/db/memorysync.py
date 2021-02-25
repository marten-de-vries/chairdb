import sortedcontainers

import collections
import contextlib
import uuid
import numbers

from .attachments import AttachmentStore
from .datatypes import Document, NotFound
from .shared import (SyncTransactionBasedDBMixin, read_docs, build_change,
                     revs_diff)
from .revtree import RevisionTree
from ..utils import InMemoryAttachment, verify_no_attachments


class SyncInMemoryDatabase(SyncTransactionBasedDBMixin):
    def __init__(self, id=None):
        self.id_sync = (id or uuid.uuid4().hex) + 'memory'
        self.update_seq_sync = 0
        self.revs_limit_sync = 1000

        # id -> document (dict)
        self._local = sortedcontainers.SortedDict(self._collate)
        # id -> (rev_tree, last_update_seq)
        self._byid = sortedcontainers.SortedDict(self._collate)
        # seq -> id (str)
        self._byseq = sortedcontainers.SortedDict()

    def _collate(self, key):  # noqa: C901
        if key is None:
            return (1,)
        if isinstance(key, bool):
            return (2, key)
        if isinstance(key, numbers.Number):
            return (3, key)
        if isinstance(key, str):
            return (4, key.casefold())  # TODO: full-blown unicode collate?
        if isinstance(key, collections.abc.Sequence):
            return (5, tuple(self._collate(k) for k in key))
        if isinstance(key, collections.abc.Mapping):
            return (6, tuple(self._collate(k) for k in key))
        raise KeyError(f'Unsupported key: {key}')

    def all_docs_sync(self, **opts):
        verify_no_attachments(opts.get('doc_opts', {}))
        with self.read_transaction_sync() as t:
            yield from t.all_docs(**opts)

    def ensure_full_commit_sync(self):
        """a no-op for an in-memory db"""

    def read_sync(self, id, **opts):
        verify_no_attachments(opts)
        with self.read_transaction_sync() as t:
            yield from t.read(id, **opts)

    @contextlib.contextmanager
    def read_transaction_sync(self):
        yield SyncReadTransaction(self._local, self._byid, self._byseq)

    @contextlib.contextmanager
    def write_transaction_sync(self):
        actions = []
        yield SyncWriteTransaction(actions)
        return self._dispatch_actions(actions)

    def _dispatch_actions(self, actions):
        # replace indices with copies such that current readers keep access to
        # the 'old' state
        self._byid = self._byid.copy()
        self._byseq = self._byseq.copy()
        self._local = self._local.copy()

        # and ('atomically') execute the actions gathered during the
        # transaction
        for action, *args in actions:
            {
                'write': self._write_impl,
                'write_local': self._write_local_impl,
            }[action](*args)

    def _write_impl(self, doc):
        # get the new document's path and check if it replaces something
        try:
            tree, last_update_seq = self._byid[doc.id]
        except KeyError:
            tree, last_update_seq = RevisionTree(), None

        full_path, old_ptr, old_i = tree.merge_with_path(doc.rev_num, doc.path)
        if not full_path:
            return  # document already in the database.

        doc_ptr = self._create_doc_ptr(doc, old_ptr)
        # insert or replace in the rev tree
        tree.update(doc.rev_num, full_path, doc_ptr, old_i,
                    self.revs_limit_sync)

        self.update_seq_sync += 1
        # actual insertion by updating the document info in the indices
        self._byid[doc.id] = tree, self.update_seq_sync
        # update the by seq index by first removing a previous reference to the
        # current document (if there is one), and then inserting a new one.
        if last_update_seq:
            del self._byseq[last_update_seq]
        self._byseq[self.update_seq_sync] = doc.id

        # Let subclass(es) know stuff changed
        self._updated()

    def _create_doc_ptr(self, doc, old_ptr):
        """A doc_ptr is a (body, attachments) tuple for the in-memory case."""

        doc_ptr = None
        if not doc.is_deleted:
            try:
                _, att_store = old_ptr
            except TypeError:
                att_store = AttachmentStore()
            _, new = att_store.merge(doc.attachments)
            for name, attachment in new:
                # first read
                data_ptr = b''.join(attachment)
                # then store. In that order, or attachment.meta isn't
                # necessarily up-to-date yet
                att_store.add(name, attachment.meta, data_ptr)
            doc_ptr = (doc.body.copy(), att_store)
        return doc_ptr

    def _write_local_impl(self, id, doc):
        if doc is None:
            self._local.pop(id, None)  # silence KeyError
        else:
            self._local[id] = doc


class SyncReadTransaction:
    def __init__(self, local, byid, byseq):
        self._local = local
        self._byid = byid
        self._byseq = byseq

    def all_docs(self, *, start_key=None, end_key=None, descending=False,
                 doc_opts=dict(body=False, att_names=None, atts_since=None)):

        iter = self._byid.irange(start_key, end_key, reverse=descending)
        for id in iter:
            rev_tree, _ = self._byid[id]
            branch = rev_tree.winner()
            if branch.leaf_doc_ptr:  # not deleted
                yield self._read_doc(id, branch, **doc_opts)

    def changes(self, since=None):
        """If we ever support style='main_only' then storing winner metadata in
        the byseq index would make sense. Now, not so much. We need to query
        the by_id index for the revision tree anyway...

        """
        for seq in self._byseq.irange(minimum=since, inclusive=(False, False)):
            id = self._byseq[seq]
            rev_tree, _ = self._byid[id]
            yield build_change(id, seq, rev_tree)

    def read(self, id, *, revs=None, body=True, att_names=None,
             atts_since=None):
        try:
            # find it using the 'by id' index
            rev_tree, _ = self._byid[id]
        except KeyError as e:
            raise NotFound(id) from e

        for branch in read_docs(id, revs, rev_tree):
            yield self._read_doc(id, branch, body, att_names, atts_since)

    def _read_doc(self, id, branch, body=True, att_names=None,
                  atts_since=None):
        # TODO: put more 'Document'-generating code in shared.read_docs?
        rev_num = branch.leaf_rev_num
        deleted = not branch.leaf_doc_ptr
        if deleted:
            doc_body, atts = None, None
        else:
            doc_body, att_store = branch.leaf_doc_ptr
            doc_body = doc_body.copy()
            if not body:
                doc_body = None

            atts, todo = att_store.read(branch, att_names, atts_since)
            for name, info in todo:
                atts[name] = InMemoryAttachment(info.meta, info.data_ptr)

        return Document(id, rev_num, branch.path, doc_body, atts, deleted)

    def read_local(self, id):
        return self._local.get(id)

    def revs_diff(self, id, revs):
        try:
            rev_tree, _ = self._byid[id]
        except KeyError:
            rev_tree = RevisionTree()
        return revs_diff(id, revs, rev_tree)


class SyncWriteTransaction:
    def __init__(self, actions):
        self._actions = actions

    def write(self, doc):
        self._actions.append(('write', doc))

    def write_local(self, id, doc):
        self._actions.append(('write_local', id, doc))
