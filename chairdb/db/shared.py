import asyncio

from .revtree import RevisionTree
from .datatypes import Change, Missing, Document


def build_change(id, seq, rev_tree, winning_branch_idx):
    winning_branch = rev_tree[winning_branch_idx]
    deleted = winning_branch.leaf_doc_ptr is None
    leaf_revs = [rev_tuple(b, b.leaf_rev_num) for b in rev_tree.branches()]
    return Change(id, seq, deleted, leaf_revs)


def revs_diff(id, revs, rev_tree):
    if rev_tree:
        revs_in_db = (rev_tuple(b, rn) for b, rn in rev_tree.all_revs())
    else:
        revs_in_db = ()
    return Missing(id, set(revs).difference(revs_in_db))


def rev_tuple(branch, rev_num):
    return rev_num, branch.path[branch.index(rev_num)]


def read_docs(id, revs, rev_tree, winning_branch_idx):
    if revs == 'winner':
        # winner information is passed in directly
        branch = rev_tree[winning_branch_idx]
        yield Document(id, *branch)
    else:
        # ... walk the revision tree
        yield from read_revs(id, revs, rev_tree)


def read_revs(id, revs, rev_tree):
    if revs == 'all':
        # all leafs
        for branch in rev_tree.branches():
            yield Document(id, *branch)
    else:
        # search for specific revisions
        for rev in revs:
            for branch in rev_tree.find(*rev):
                yield Document(id, *branch)


def update_doc(doc, rev_tree, revs_limit):
    if rev_tree is None:
        rev_tree = RevisionTree([])  # new empty tree

    rev_tree.merge_with_path(doc.rev_num, doc.path, doc.body, revs_limit)
    return rev_tree, rev_tree.winner_idx()


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
