import asyncio

from .datatypes import Change, Missing


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
    - self._update_event, an asyncio.Event that gets set whenever a change to
      a document was made. Immediately afterward, the used event needs to be
      replaced with a fresh one.

    """
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
