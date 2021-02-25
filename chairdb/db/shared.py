import anyio

import contextlib

from .datatypes import Change, Missing


def build_change(id, seq, rev_tree):
    deleted = rev_tree.winner().leaf_doc_ptr is None
    leaf_revs = [branch.leaf_rev_tuple for branch in rev_tree.branches()]
    return Change(id, seq, deleted, leaf_revs)


def revs_diff(id, revs, rev_tree):
    missing, possible_ancestors = set(), set()
    for rev in revs:
        is_missing, new_possible_ancestors = rev_tree.diff(*rev)
        if is_missing:
            missing.add(rev)
            possible_ancestors.update(new_possible_ancestors)
    return Missing(id, missing, possible_ancestors)


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


async def as_future_result(value):
    await anyio.sleep(0)
    return value


def _doc_reader_proxy(method_name):
    @contextlib.asynccontextmanager
    async def proxy(self, *args, **kwargs):
        async with self.read_transaction() as t:
            yield getattr(t, method_name)(*args, **kwargs)
    return proxy


def _writer_proxy(method_name):
    async def proxy(self, *args, **kwargs):
        async with self.write_transaction() as t:
            getattr(t, method_name)(*args, **kwargs)
    return proxy


class TransactionBasedDBMixin:
    all_docs_with_attachments = _doc_reader_proxy('all_docs')
    read_with_attachments = _doc_reader_proxy('read')

    write = _writer_proxy('write')
    write_local = _writer_proxy('write_local')

    async def _changes(self, since=None):
        async with self.read_transaction() as t:
            async for change in t.changes(since):
                yield change

    async def read_local(self, id):
        async with self.read_transaction() as t:
            return await t.read_local(id)

    async def revs_diff(self, requested):
        async for id, revs in requested:
            async with self.read_transaction() as t:
                yield await t.revs_diff(id, revs)


def _sync_doc_reader_proxy(method_name):
    @contextlib.contextmanager
    def proxy(self, *args, **kwargs):
        with self.read_transaction_sync() as t:
            yield getattr(t, method_name)(*args, **kwargs)
    return proxy


def _sync_reader_proxy(method_name):
    def proxy(self, *args, **kwargs):
        with self.read_transaction_sync() as t:
            return getattr(t, method_name)(*args, **kwargs)
    return proxy


def _sync_writer_proxy(method_name):
    def proxy(self, *args, **kwargs):
        with self.write_transaction_sync() as t:
            getattr(t, method_name)(*args, **kwargs)
    return proxy


class SyncTransactionBasedDBMixin:
    """Implements the main DB reading/writing methods using
    read_transaction_sync and write_transaction_sync calls.

    """
    all_docs_with_attachments_sync = _sync_doc_reader_proxy('all_docs')
    read_with_attachments_sync = _sync_doc_reader_proxy('read')

    read_local_sync = _sync_reader_proxy('read_local')
    revs_diff_sync = _sync_reader_proxy('revs_diff')

    write_sync = _sync_writer_proxy('write')
    write_local_sync = _sync_writer_proxy('write_local')

    def changes_sync(self, since=None):
        with self.read_transaction_sync() as t:
            yield from t.changes(since)


class ContinuousChangesMixin:
    """Requires the following to be implemented:

    - (async) self._changes(since), which gives changes non-continuously

    Also requires that the caller calls:

    - self._updated(), whenever a new (non-local) document has been written

    """
    def _updated(self):
        if hasattr(self, '_update_event'):
            self._update_event.set()
            del self._update_event

    async def changes(self, since=None, continuous=False):
        """"Like CouchDB's _changes with style=all_docs"""

        while True:
            # send (new) changes to the caller
            async for change in self._changes(since):
                since = change.seq
                yield change

            if not continuous:
                # stop immediately.
                break
            # wait for new changes to come available, then loop
            if not hasattr(self, '_update_event'):
                self._update_event = anyio.create_event()
            await self._update_event.wait()
