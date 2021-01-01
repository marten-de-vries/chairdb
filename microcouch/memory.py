import sortedcontainers

import asyncio
import typing

from .revtree import Leaf, RevisionTree
from .utils import rev as _rev, parse_rev as _parse_rev


class DocumentInfo(typing.NamedTuple):
    winner_rev_num: int
    winner_leaf: Leaf
    rev_tree: RevisionTree
    last_update_seq: int


class Change(typing.NamedTuple):
    id: str
    seq: int
    deleted: bool
    leaf_revs: list


class InMemoryDatabase:
    """Offers both a synchronous and asynchronous interface. Considering the
    (memory) backend, the asynchronous interface is there only for compatibilty
    with other implementations that require it.

    """
    def __init__(self):
        self.update_seq_sync = 0
        # id (without _local) -> document (dict; without _id & _rev)
        self.local = sortedcontainers.SortedDict()
        # id -> DocumentInfo
        self._byid = sortedcontainers.SortedDict()
        # seq -> id (str)
        self._byseq = sortedcontainers.SortedDict()

    @property
    def update_seq(self):
        # hackity hack
        f = asyncio.get_event_loop().create_future()
        f.set_result(self.update_seq_sync)
        return f

    async def changes(self):
        # boring conversion code
        for change in self.changes_sync():
            yield change

    def changes_sync(self):
        # if we ever support style='main_only' then storing winner metadata in
        # the byseq index would make sense. Now, not so much. We need to query
        # the by_id index for the revision tree anyway...

        for seq, id in self._byseq.items():
            doc_info = self._byid[id]
            deleted = doc_info.winner_leaf.doc_ptr is None
            yield Change(id, seq, deleted, list(self._leaf_revs(doc_info)))

    def _leaf_revs(self, doc_info):
        for rev_num, _, leaf in doc_info.rev_tree.leafs():
            yield _rev(rev_num, leaf)

    async def revs_diff(self, remote):
        async for id, revs in remote:
            yield self.revs_diff_sync(id, revs)

    def revs_diff_sync(self, id, revs):
        try:
            doc_info = self._byid[id]
        except KeyError:
            missing = set(revs)
        else:
            missing = set(revs) - set(doc_info.rev_tree.all_revs())
        return id, {'missing': missing}

    async def write(self, docs):
        async for doc in docs:
            try:
                self.write_sync(doc)
            except (KeyError, AssertionError) as error:
                yield error

    def write_sync(self, doc):
        id = doc.pop('_id')
        revs, doc = self._prepare_doc(doc)
        rev_tree = self._update_rev_tree(id, revs, doc)
        winner_rev_num, winner_leaf = rev_tree.winner()

        self.update_seq_sync += 1
        new_update_seq = self.update_seq_sync
        self._byid[id] = DocumentInfo(winner_rev_num, winner_leaf,
                                      rev_tree, new_update_seq)
        self._byseq[new_update_seq] = id

    def _prepare_doc(self, doc):
        rev_num, rev_hash = _parse_rev(doc.pop('_rev'))
        revs = doc.pop('_revisions', {'start': rev_num, 'ids': [rev_hash]})
        assert revs['ids'][0] == rev_hash, 'Invalid _revisions'
        if doc.get('_deleted'):
            doc = None
        return revs, doc

    def _update_rev_tree(self, id, revs, doc):
        try:
            # load existing tree
            _, _, rev_tree, old_seq = self._byid[id]
        except KeyError:
            rev_tree = RevisionTree([])  # new empty tree
        else:
            del self._byseq[old_seq]

        rev_tree.merge_with_path(revs, doc)
        rev_tree.validate()  # TODO: remove?
        return rev_tree

    async def read(self, requested, include_path=False):
        async for id, revs in requested:
            for doc in self.read_sync(id, revs):
                yield doc

    def read_sync(self, id, revs, include_path=False):
        winning_rev_num, winning_leaf, rev_tree, _ = self._byid[id]
        if include_path and revs == 'winner':
            # we need the path in the tree, so search in full
            revs = [_rev(winning_rev_num, winning_leaf)]
        if revs == 'winner':
            yield self._to_doc(id, winning_rev_num, winning_leaf)
        else:
            yield from self._read_revs(id, revs, rev_tree, include_path)

    def _read_revs(self, id, revs, rev_tree, include_path):
        if revs == 'all':
            for rev_num, path, leaf in rev_tree.leafs(include_path):
                yield self._to_doc(id, rev_num, leaf, path)
        else:
            for rev_num, path, leaf in rev_tree.find(revs, include_path):
                yield self._to_doc(id, rev_num, leaf, path)

    def _to_doc(self, id, rev_num, leaf, path=None):
        doc = {'_id': id, '_rev': _rev(rev_num, leaf)}
        if leaf.doc_ptr is None:
            doc['_deleted'] = True
        else:
            doc.update(leaf.doc_ptr)
        if path is not None:
            ids = [leaf.rev_hash] + [rev_hash for _, rev_hash in path]
            doc['_revisions'] = {'start': rev_num, 'ids': ids}
        return doc
