import asyncio

from .revtree import RevisionTree
from .datatypes import Change


def build_change(id, seq, rev_tree, winning_leaf_idx):
    winning_leaf = rev_tree[winning_leaf_idx]
    deleted = winning_leaf.doc_ptr is None
    leaf_revs = [rev(*leaf) for leaf in rev_tree.leafs()]
    return Change(id, seq, deleted, leaf_revs)


def rev(leaf, rev_num):
    return f'{rev_num}-{leaf.path[leaf.index(rev_num)]}'


def parse_rev(rev):
    num, hash = rev.split('-')
    return int(num), hash


def revs_diff(id, revs, rev_tree):
    if rev_tree:
        revs_in_db = (rev(*r) for r in rev_tree.all_revs())
    else:
        revs_in_db = ()
    return id, {'missing': set(revs).difference(revs_in_db)}


def prepare_doc_write(doc):
    """Normalize _revisions field & handle delete flag"""

    id = doc.pop('_id')
    if is_local(id):
        revs = None
    else:
        rev_num, rev_hash = parse_rev(doc.pop('_rev'))
        revs = doc.pop('_revisions', {'start': rev_num, 'ids': [rev_hash]})
        assert revs['ids'][0] == rev_hash, 'Invalid _revisions'
    if doc.get('_deleted'):
        doc = None
    return id, revs, doc


def is_local(id):
    return id.startswith('_local')


def read_docs(id, revs, include_path, rev_tree, winning_leaf_idx):
    if revs == 'winner':
        # the information is stored in the DocumentInfo directly
        leaf = rev_tree[winning_leaf_idx]
        yield to_doc(id, leaf, leaf.rev_num, include_path)
    else:
        # ... walk the revision tree
        yield from read_revs(id, revs, rev_tree, include_path)


def read_revs(id, revs, rev_tree, include_path):
    if revs == 'all':
        # all leafs
        for leaf, rev_num in rev_tree.leafs():
            yield to_doc(id, leaf, rev_num, include_path)
    else:
        revs = {parse_rev(rev) for rev in revs}
        # search for specific revisions
        for leaf, rev_num in rev_tree.find(revs):
            yield to_doc(id, leaf, rev_num, include_path)


def to_doc(id, leaf, rev_num, include_path):
    """Reconstruct a CouchDB-compatible JSON document from the gathered
    information

    """
    doc = {'_id': id, '_rev': rev(leaf, rev_num)}
    if leaf.doc_ptr is None:
        doc['_deleted'] = True
    else:
        doc.update(leaf.doc_ptr)
    if include_path:
        ids = leaf.path[leaf.index(rev_num):]
        doc['_revisions'] = {'start': rev_num, 'ids': ids}
    return doc


def to_local_doc(id, revs, base):
    assert revs == 'winner'

    # the revision is fixed
    return {**base, '_id': id, '_rev': '0-1'}


def update_doc(id, revs, doc, rev_tree, revs_limit):
    if rev_tree is None:
        rev_tree = RevisionTree([])  # new empty tree

    rev_tree.merge_with_path(revs['start'], revs['ids'], doc, revs_limit)
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
