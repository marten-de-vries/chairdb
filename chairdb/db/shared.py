import asyncio

from .datatypes import Change, Missing, NotFound
from ..utils import to_list


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

    Views and purging are not implemented, but everything essential for
    replication implemented.

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
                await self._syncify_attachments(doc)
                self.write_sync(doc)
            except Exception as exc:
                yield exc

    async def _syncify_attachments(self, doc):
        if doc.attachments:
            tasks = []
            for name, att in doc.attachments.items():
                if not att.is_stub:
                    tasks.append(self._syncify_att(doc, name, att))
            await asyncio.gather(*tasks)

    async def _syncify_att(self, doc, name, att):
        data_list = await to_list(att)
        doc.attachments[name] = SyncAttachment(att.meta, data_list)

    async def read_local(self, id):
        return self.read_local_sync(id)

    async def read(self, requested):
        """Like CouchDB's GET dbname/docid?latest=true, but allows asking for
        multiple documents at once. 'requested' is an (async) iterable of
        id, opts tuples. 'id' is the document id. `opts["revs"]`` specify which
        version(s) of said document you want to access. 'revs' can be:

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

        """
        async for id, opts in requested:
            try:
                for doc in self.read_sync(id, **opts):
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


class SyncAttachment(list):
    def __init__(self, meta, data_list):
        super().__init__(data_list)

        self.is_stub = False
        self.meta = meta
