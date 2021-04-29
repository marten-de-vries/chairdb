"""A bunch of convenience APIs around the (actual) functionality implemented in
abstract.py

"""
import anyio

import contextlib

from ..utils import verify_no_attachments


def doc_reader_proxy(method_name):
    async def proxy(self, *args, **kwargs):
        async with self.read_transaction() as t:
            yield getattr(t, method_name)(*args, **kwargs)
    return contextlib.asynccontextmanager(proxy)


def property_reader_proxy(property_name):
    def proxy(self):
        async def get():
            async with self.read_transaction() as t:
                return await getattr(t, property_name)
        return get()
    return property(proxy)


def writer_proxy(method_name):
    async def proxy(self, *args, **kwargs):
        async with self.write_transaction() as t:
            getattr(t, method_name)(*args, **kwargs)
    return proxy


class TransactionBasedDBMixin:
    all_docs_with_attachments = doc_reader_proxy('all_docs')
    read_with_attachments = doc_reader_proxy('read')

    update_seq = property_reader_proxy('update_seq')
    update_seq.__doc__ = """
    Each database modification increases this. Starting at zero by
    convention.

    """.lstrip()
    revs_limit = property_reader_proxy('revs_limit')

    write = writer_proxy('write')
    write_local = writer_proxy('write_local')

    async def all_docs(self, **opts):
        verify_no_attachments(opts.get('doc_opts', {}))

        async with self.read_transaction() as t:
            async for doc in t.all_docs(**opts):
                yield doc

    async def read(self, id, **opts):
        """Like CouchDB's GET dbname/docid?latest=true. 'id' is the document
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
        using the opts['atts'] option, which takes an AttachmentSelector as
        argument:

        - AttachmentSelector(names=(...)) allows you to specify a tuple of
          attachment names to retrieve. Defaults to the empty tuple.
        - AttachmentSelector(since_revs=(...)) allows you to specify a tuple of
          revisions. Any attachment that was added later is returned. Note that
          you can use this to return all attachments by setting it to an empty
          list (a shortcut for this is 'AttachmentSelector.all()'). Defaults to
          None. (Which differs from an empty list!)

        Finally, opts['body']=True determines whether to retrieve the document
        body. If you only need an attachment, or revision information, you can
        set it to False. Similarly, opts['atts']=None will not even load
        attachment stubs.

        """
        verify_no_attachments(opts)

        async with self.read_transaction() as t:
            async for doc in t.read(id, **opts):
                yield doc

    async def _changes(self, since=None):
        async with self.read_transaction() as t:
            async for change in t.changes(since):
                yield change

    async def read_local(self, id):
        async with self.read_transaction() as t:
            return await t.read_local(id)

    async def revs_diff(self, requested):
        async for id, revs in requested:
            yield await self.single_revs_diff(id, revs)

    async def single_revs_diff(self, id, revs):
        async with self.read_transaction() as t:
            return await t.single_revs_diff(id, revs)

    async def set_revs_limit(self, limit):
        async with self.write_transaction() as t:
            t.revs_limit = limit


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
                self._update_event = anyio.Event()
            await self._update_event.wait()
