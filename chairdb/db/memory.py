import anyio

import contextlib

from ..utils import to_list, verify_no_attachments
from .memorysync import SyncInMemoryDatabase, SyncWriteTransaction
from .shared import (ContinuousChangesMixin, TransactionBasedDBMixin,
                     as_future_result)


class InMemoryDatabase(SyncInMemoryDatabase, TransactionBasedDBMixin,
                       ContinuousChangesMixin):
    """An in-memory implementation of a CouchDB-compatible database.

    The database does not keep the documents for non-leaf revisions for
    simplicity, which has the nice side-effect of effectively auto-compacting
    the database continously. This means you cannot use revisions as a history
    mechanism, though. (But that isn't recommended anyway.)

    Purging is not implemented, but everything essential for replication is.

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
    async def all_docs(self, **opts):
        verify_no_attachments(opts.get('doc_opts', {}))

        async with self.read_transaction() as t:
            async for doc in t.all_docs(**opts):
                yield doc

    async def ensure_full_commit(self):
        """a no-op for an in-memory db"""

    @property
    def id(self):
        """For identification of this specific database during replication. For
        a (volatile) in-memory database, a random uuid (i.e. the default) is
        actually quite a reasonable choice.

        """
        return as_future_result(self.id_sync)

    async def read(self, id, **opts):
        verify_no_attachments(opts)

        async with self.read_transaction() as t:
            async for doc in t.read(id, **opts):
                yield doc

    @contextlib.asynccontextmanager
    async def read_transaction(self):
        """Not supported by all databases, but required for (local) views."""

        with self.read_transaction_sync() as t:
            yield ReadTransaction(t)

    @property
    def revs_limit(self):
        return as_future_result(self.revs_limit_sync)

    async def set_revs_limit(self, value):
        self.revs_limit_sync = value

    @property
    def update_seq(self):
        """Each database modification increases this. Starting at zero by
        convention.

        """
        return as_future_result(self.update_seq_sync)

    @contextlib.asynccontextmanager
    async def write_transaction(self):
        """Not supported by all databases but required for (local) views."""

        if not hasattr(self, '_write_lock'):
            self._write_lock = anyio.create_lock()

        actions = []
        async with self._write_lock:
            async with anyio.create_task_group() as tg:
                yield WriteTransaction(tg, actions)
            self._dispatch_actions(actions)


def asyncify_rt_generator(method_name):
    async def proxy(self, *args, **kwargs):
        for item in getattr(self._t, method_name)(*args, **kwargs):
            yield item
    return proxy


def asyncify_rt_method(method_name):
    async def proxy(self, *args, **kwargs):
        return getattr(self._t, method_name)(*args, **kwargs)
    return proxy


class ReadTransaction:
    def __init__(self, t):
        self._t = t

    all_docs = asyncify_rt_generator('all_docs')
    changes = asyncify_rt_generator('changes')
    read = asyncify_rt_generator('read')
    read.__doc__ = """
        Like CouchDB's GET dbname/docid?latest=true. 'id' is the document
        id. `opts["revs"]`` specify which version(s) of said document you want
        to access. 'revs' can be:

        - 'None' (what you would get by default from CouchDB, i.e. the winner)
        - 'all' (what you would get from CouchDB by include 'open_revs=all',
          i.e. all leafs)
        - a list of revisions (which you would get from CouchDB when manually
          specifying 'open_revs=[...]'.

        Note that, for non-winner values of revs, this can return multiple
        document leafs. That's why this method is an (async) generator.

        By default, attachment contents are not retrieved. Only 'stub'
        information about them is returned. You can force attachment retrieval
        using two options:
        - revs['att_names'] allows you to specify a list of attachment names to
          retrieve. Defaults to None.
        - revs['atts_since'] allows you to specify a list of revisions. Any
          attachment that was added later is returned. Note that you can use
          this to return all attachments by setting it to an empty list.
          Defaults to None. (Which differs from an empty list!)

        Finally, opts['body']=True determines whether to retrieve the document
        body. If you only need an attachment, or revision information, you can
        set it to False.

        """.lstrip()
    read_local = asyncify_rt_method('read_local')
    revs_diff = asyncify_rt_method('revs_diff')


class WriteTransaction(SyncWriteTransaction):
    def __init__(self, tg, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._tg = tg

    def write(self, doc):
        for name, att in (doc.attachments or {}).items():
            if not att.is_stub:
                self._tg.spawn(self._syncify_att, doc, name, att)

        super().write(doc)

    async def _syncify_att(self, doc, name, att):
        data_list = await to_list(att)
        doc.attachments[name] = SyncAttachment(att.meta, data_list)


class SyncAttachment(list):
    def __init__(self, meta, data_list):
        super().__init__(data_list)

        self.is_stub = False
        self.meta = meta
