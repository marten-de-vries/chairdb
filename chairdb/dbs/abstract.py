import anyio

import contextlib
import uuid

from .writetransaction import WriteTransaction
from .readtransaction import ReadTransaction
from ..errors import Conflict
from .shared import (update_atts, as_future_result, get_revs_limit, chunk_id,
                     get_rev_tree, decode_att_store)


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
                if action == 'write_local':
                    t.write_local(*args)
                else:
                    await self._write_impl(t, *args)
        self._updated()

    async def _write_impl(self, t, chunk_info, doc, new_edit):
        if new_edit:
            """TODO: calculate & insert new rev hash"""

        # get the new document's path and check if it replaces something
        tree = await get_rev_tree(t, doc.id)

        state, *args = tree.merge_with_path(doc.rev_num, doc.path)
        # states: already_inserted, replace_insert, fork_insert, new_insert
        conflict = (state == 'fork_insert' and new_edit)
        if state == 'already_inserted' or conflict:
            # remove one of the now doubly inserted attachments
            for name, (att_id, chunk_ends) in chunk_info.items():
                for i in range(len(chunk_ends)):
                    t.write_local(chunk_id(att_id, i), None)
            if conflict:
                raise Conflict()
            return
        old_att_store = {}
        if state == 'replace_insert':
            *args, old_doc_ptr = args
            t.write_local(f'_body_{old_doc_ptr}', None)
            encoded = await t.read_local(f'_att_store_{old_doc_ptr}')
            if encoded:
                old_att_store = decode_att_store(encoded)
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
