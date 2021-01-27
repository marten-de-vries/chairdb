import asyncio

from .datatypes import Change, Missing, NotFound


def build_change(id, seq, rev_tree):
    deleted = rev_tree.winner().leaf_doc_ptr is None
    leaf_revs = [rev_tuple(b, b.leaf_rev_num) for b in rev_tree.branches()]
    return Change(id, seq, deleted, leaf_revs)


def revs_diff(id, revs, rev_tree):
    revs_in_db = (rev_tuple(b, rn) for b, rn in rev_tree.all_revs())
    return Missing(id, set(revs).difference(revs_in_db))


def rev_tuple(branch, rev_num):
    return rev_num, branch.path[branch.index(rev_num)]


def read_docs(id, revs, rev_tree):
    # ... walk the revision tree
    if revs == 'winner':
        yield rev_tree.winner()
    else:
        if revs == 'all':
            # all leafs
            yield from rev_tree.branches()
        else:
            for rev in revs:
                # search for specific revisions
                yield from rev_tree.find(*rev)


class ContinuousChangesMixin:
    """Requires the following to be implemented:

    - (async) self._changes(since), which gives changes non-continuously

    Also requires that the caller calls:

    - self._updated(), whenever a new (non-local) document has been written

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._update_event = asyncio.Event()

    def _updated(self):
        self._update_event.set()
        self._update_event.clear()

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
            await self._update_event.wait()


def as_future_result(value):
    future = asyncio.get_event_loop().create_future()
    future.set_result(value)
    return future


class AsyncDatabaseMixin(ContinuousChangesMixin):
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

    async def write_local(self, id, doc):
        self.write_local_sync(id, doc)

    async def write(self, docs):
        """Like CouchDB's _bulk_docs with new_edits=false"""

        async for doc in docs:
            try:
                self.write_sync(doc)
            except Exception as exc:
                yield exc

    async def read_local(self, id):
        return self.read_local_sync(id)

    async def read(self, requested):
        """Like CouchDB's GET dbname/docid?latest=true, but allows asking for
        multiple documents at once. 'reqested' is an (async) iterable of
        (id, revs) tuples. 'id' is the document id. 'revs' specify which
        version(s) of said document you want to access. 'revs' can be:

        - 'winner' (what you would get by default from CouchDB)
        - 'all' (what you would get from CouchDB by include 'open_revs=all',
          i.e. all leafs)
        - a list of revisions (which you would get from CouchDB when manually
          specifying 'open_revs=[...]'.

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
