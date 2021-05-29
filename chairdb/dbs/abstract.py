import anyio

import contextlib
import uuid
import typing

from ..errors import Conflict, NotFound
from ..datatypes import (Change, Missing, Document, AttachmentSelector,
                         AttachmentMetadata)
from ..utils import aenumerate, as_future_result

from .revtree import RevisionTree
from .attachments import update_atts, chunk_id, read_atts, slice_att


class AbstractDatabase:
    """An implementation of a CouchDB-compatible database.

    The database does not keep the documents for non-leaf revisions for
    simplicity, which has the nice side-effect of effectively auto-compacting
    the database continously. This means you cannot use revisions as a history
    mechanism, though. (But that isn't recommended anyway.)

    Purging is not implemented, but everything essential for replication is.

    Note that writing acts similar to _bulk_docs with new_edits=false. So you
    need to manually generate new revisions (and check for conflicts, I guess).

    """
    def __init__(self, *args, **kwargs):
        self._backend = self._Backend(*args, **kwargs)

    async def create(self):
        return await self._backend.create()

    async def ensure_full_commit(self):
        """a no-op, at least for now"""

    @property
    def id(self):
        """For identification of this specific database during replication. For
        a (volatile) in-memory database, a random uuid is used.

        """
        return as_future_result(self._backend.id)

    @contextlib.asynccontextmanager
    async def read_transaction(self):
        """Not supported by remote databases, but required for (local) views.

        """
        async with self._backend.read_transaction() as t:
            yield ReadTransaction(t)

    @contextlib.asynccontextmanager
    async def write_transaction(self):
        """Not supported by remote databases, but required for (local) views.

        """
        actions = []
        async with anyio.create_task_group() as tg:
            yield WriteTransaction(tg, self._backend, actions)
        async with self._backend.write_transaction() as t:
            for action, *args in actions:
                if action == 'purge':
                    await self._purge_impl(t, *args)
                elif action == 'write_local':
                    t.write_local(*args)
                else:
                    await self._write_impl(t, *args)
        self._updated()

    async def _purge_impl(self, t, id, leaf_revisions):
        tree = await get_rev_tree(t, id)
        for purged_branch in tree.purge(id, leaf_revisions):
            t.write_local(f'_body_{purged_branch.leaf_doc_ptr}')
            t.write_local(f'_att_store_{purged_branch.leaf_doc_ptr}')
            # TODO: remove unreferenced chunks (see _write_impl)

        # tree can now be the empty list, but that should be handled by the
        # backend
        t.write(id, tree)
        # TODO: invalidate (or update) views

    async def _write_impl(self, t, chunk_info, doc, check_conflict):
        # get the new document's path and check if it replaces something
        tree = await get_rev_tree(t, doc.id)
        state, *args = tree.merge_with_path(doc.rev_num, doc.path)

        # states: already_inserted, replace_insert, fork_insert, new_insert
        conflict = (check_conflict and state == 'fork_insert')
        if state == 'already_inserted' or conflict:
            return await self._handle_early_return(t, chunk_info, conflict)
        old_att_store = {}
        if state == 'replace_insert':
            *args, old_doc_ptr = args
            t.write_local(f'_body_{old_doc_ptr}', None)
            old_att_store = await t.read_local(f'_att_store_{old_doc_ptr}')
            t.write_local(f'_attt_store_{old_doc_ptr}', None)
            # TODO: how to clean up old attachments? reference counting
            # TODO: somehow? For now, just leave them in (ignore)...
        if doc.is_deleted:
            doc_ptr = None
        else:
            assert doc.body is not None
            doc_ptr = uuid.uuid1().hex
            t.write_local(f'_body_{doc_ptr}', doc.body)

            assert doc.attachments is not None
            att_store = update_atts(old_att_store, doc.attachments, chunk_info)
            t.write_local(f'_att_store_{doc_ptr}', att_store)

        # insert or replace in the rev tree
        tree.update(await get_revs_limit(t), doc_ptr, doc.rev_num, *args)

        t.write(doc.id, tree)

    async def _handle_early_return(self, t, chunk_info, conflict):
        # remove the newest of the now doubly inserted attachments
        for name, (att_id, chunk_ends) in chunk_info.items():
            for i in range(len(chunk_ends)):
                t.write_local(chunk_id(att_id, i), None)
        if conflict:
            raise Conflict()


class ReadTransaction:
    def __init__(self, t):
        self._t = t

    @property
    def update_seq(self):
        return self._t.update_seq

    @property
    def revs_limit(self):
        return get_revs_limit(self._t)

    async def all_docs(self, *, start_key=None, end_key=None, descending=False,
                       doc_opts=dict(body=False, atts=None)):
        rows = self._t.all_docs(start_key, end_key, descending)
        async for id, rev_tree in rows:
            for branch in all_docs_branch(rev_tree):
                yield await self._read_doc(id, branch, **doc_opts)

    def all_local_docs(self, *, start_key=None, end_key=None,
                       descending=False):
        return self._t.all_local_docs(start_key, end_key, descending)

    async def changes(self, since=None):
        async for seq, id, rev_tree in self._t.changes(since):
            yield build_change(id, seq, rev_tree)

    async def read(self, id, *, revs=None, body=True,
                   atts=AttachmentSelector()):
        rev_tree = await self._t.read(id)
        for branch in read_docs(id, revs, rev_tree):
            yield await self._read_doc(id, branch, body, atts)

    async def _read_doc(self, id, branch, body, atts):
        doc_ptr = branch.leaf_doc_ptr
        deleted = not doc_ptr

        doc_body, doc_atts = None, None
        if not deleted:
            if body:
                doc_body = await self._t.read_local(f'_body_{doc_ptr}')
            if atts:
                att_store = await self._t.read_local(f'_att_store_{doc_ptr}')
                doc_atts, todo = read_atts(att_store, branch, atts)
                for name, info in todo:
                    doc_atts[name] = Attachment(self, info.meta, info.data_ptr)

        return Document(id, branch.leaf_rev_num, branch.path, doc_body,
                        doc_atts, deleted)

    def read_local(self, id):
        return self._t.read_local(id)

    async def single_revs_diff(self, id, revs):
        return revs_diff(id, revs, await get_rev_tree(self._t, id))


async def get_revs_limit(t):
    return await t.read_local('_revs_limit') or 1000  # default


def all_docs_branch(rev_tree):
    branch = rev_tree.winner()
    if branch.leaf_doc_ptr:  # not deleted
        yield branch


def build_change(id, seq, rev_tree):
    deleted = rev_tree.winner().leaf_doc_ptr is None
    leaf_revs = [branch.leaf_rev_tuple for branch in rev_tree.branches()]
    return Change(id, seq, deleted, leaf_revs)


def read_docs(id, revs, rev_tree):
    # ... walk the revision tree
    if revs is None:
        yield rev_tree.winner()
    elif revs == 'all':
        # all leafs
        yield from rev_tree.branches()
    else:
        # some leafs
        for rev in revs:
            yield from rev_tree.find(*rev)


async def get_rev_tree(t, id):
    try:
        return await t.read(id)
    except NotFound:
        return RevisionTree()


def revs_diff(id, revs, rev_tree):
    missing, possible_ancestors = set(), set()
    for rev in revs:
        is_missing, new_possible_ancestors = rev_tree.diff(*rev)
        if is_missing:
            missing.add(rev)
            possible_ancestors.update(new_possible_ancestors)
    return Missing(id, missing, possible_ancestors)


class Attachment(typing.NamedTuple):
    t: ReadTransaction
    meta: AttachmentMetadata
    data_ptr: typing.Tuple[str, typing.List[int]]
    is_stub: bool = False

    def __aiter__(self):
        return self[:]

    async def __getitem__(self, s):
        sk, ek, start_offset, end_offset, last_i = slice_att(self.data_ptr, s)

        rows = self.t.all_local_docs(start_key=sk, end_key=ek)
        async for i, (id, blob) in aenumerate(rows):
            start = start_offset if i == 0 else None
            end = end_offset if i == last_i else None
            yield blob[start:end]


class WriteTransaction:
    def __init__(self, tg, backend, actions):
        self._tg = tg
        self._backend = backend
        self._actions = actions

    def write(self, doc, check_conflict=False):
        chunk_info = {}
        for name, att in (doc.attachments or {}).items():
            if not att.is_stub:
                self._tg.start_soon(self._store_att, chunk_info, name, att)
        self._actions.append(('write', chunk_info, doc, check_conflict))

    async def _store_att(self, chunk_info, name, att):
        # we keep track of the total attachment size so far after each chunk to
        # make efficient retrieval of parts of the attachment possible using
        # bisection
        att_id = uuid.uuid1().hex
        current_end = 0
        chunk_ends = []
        async for i, chunk in aenumerate(att):
            async with self._backend.write_transaction() as t:
                t.write_local(chunk_id(att_id, i), chunk)
            current_end += len(chunk)
            chunk_ends.append(current_end)
        chunk_info[name] = att_id, chunk_ends

    def write_local(self, id, doc):
        self._actions.append(('write_local', id, doc))

    revs_limit = property()

    @revs_limit.setter
    def revs_limit(self, value):
        self._actions.append(('write_local', '_revs_limit', value))

    def purge(self, id, leaf_revisions):
        self._actions.append(('purge', id, leaf_revisions))
