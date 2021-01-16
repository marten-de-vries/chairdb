import bisect
import typing


class Leaf(typing.NamedTuple):
    """A Leaf node, consisting of a revision number, it's parent revision
    hashes and a document (or None if deleted).

    """
    rev_num: int
    path: list
    doc_ptr: typing.Optional[dict]

    def index(self, rev_num):
        """Convert a revision number to a Leaf.path index"""

        return self.rev_num - rev_num


class RevisionTree(list):
    """A revision tree like:

    '3-a' -> '4-b'

    '1-c' -> '2-e' -> '3-f'
          -> '2-d'

    can be represented using this class as:

    RevisionTree([
        Leaf(2, ['d', 'c'], {}),
        Leaf(3, ['f', 'e', 'c'], {}),
        Leaf(4, ['b', 'a'], {}),
    ])

    Note that the 'longest' branches (as they were before pruning) come last.
    If two branches are of equal length, they should be sorted by revision hash
    (from low -> high). This simplifies winner determination.

    """
    def __init__(self, children):
        super().__init__(children)

        # used to keep sorted by leaf's revision number and hash
        self._keys = [self._by_max_rev(leaf) for leaf in self]

    def _by_max_rev(self, leaf):
        return leaf.rev_num, leaf.path[0]

    def merge_with_path(self, doc_rev_num, doc_path, doc, revs_limit=1000):
        """Merges a document into the revision tree, storing 'doc' into a leaf
        node (assuming the location pointed at by 'rev_num' and 'path' would in
        fact be a leaf node, which is not the case if a document has already
        been replaced by a newer version). 'doc_rev_num' is the revision number
        of the document. 'doc_path' is a list of revision hashes. The first
        hash is the last (i.e. current) revision of the document, while the
        last one is its earliest known parent revision.

        A maximum of 'revs_limit' old revisions are kept.

        """
        for i in range(len(self) - 1, -1, -1):
            leaf = self[i]
            # 1. check if already in tree. E.g.:
            #
            # leaf.rev_num = 5
            # leaf.path = ['e', 'd', 'c']
            #
            # doc_rev_num = 3
            # doc_path = ['c', 'b', 'a']
            j = leaf.index(doc_rev_num)
            if 0 <= j < len(leaf.path) and leaf.path[j] == doc_path[0]:
                return  # it is. Done.

            # 2. extend leaf if possible. E.g.:
            #
            # leaf.rev_num = 3
            # leaf.path = ['c', 'b', 'a']
            # doc_rev_num = 5
            # doc_path = ['e', 'd', 'c', 'b']
            k = doc_rev_num - leaf.rev_num
            if 0 <= k < len(doc_path) and doc_path[k] == leaf.path[0]:
                full_path = doc_path[:k] + leaf.path
                del self[i]
                del self._keys[i]
                self._insert_leaf(doc_rev_num, full_path, doc, revs_limit)
                return  # it is. Done.

        # otherwise insert as a new leaf branch:
        self._insert_as_new_branch(doc_rev_num, doc_path, doc, revs_limit)

    def _insert_as_new_branch(self, doc_rev_num, doc_path, doc, revs_limit):
        for leaf, leaf_rev_num in self.leafs():
            # 3. try to find common history
            start_leaf_rev_num = leaf_rev_num + 1 - len(leaf.path)
            start_doc_rev_num = doc_rev_num + 1 - len(doc_path)
            maybe_common_rev_num = max(start_leaf_rev_num, start_doc_rev_num)

            leaf_i = leaf.index(maybe_common_rev_num)
            doc_i = doc_rev_num - maybe_common_rev_num

            common_rev = (
                0 <= leaf_i < len(leaf.path) and
                0 <= doc_i < len(doc_path) and
                leaf.path[leaf_i] == doc_path[doc_i]
            )
            if common_rev:
                # success, combine both halves into a 'full_path'
                full_path = doc_path[:doc_i] + leaf.path[leaf_i:]
                break
        else:
            # 4. a new branch without shared history
            full_path = doc_path

        self._insert_leaf(doc_rev_num, full_path, doc, revs_limit)

    def _insert_leaf(self, doc_rev_num, full_path, doc, revs_limit):
        # stem using revs_limit
        assert revs_limit > 0
        del full_path[revs_limit:]

        # actual insertion using bisection
        leaf = Leaf(doc_rev_num, full_path, doc)
        key = self._by_max_rev(leaf)
        i = bisect.bisect(self._keys, key)
        self._keys.insert(i, key)
        self.insert(i, leaf)

    def find(self, revs):
        """For each revision number in 'revs', find the leaf documents of the
        branches pointed to by said revisions.

        """
        for leaf, leaf_rev_num in self.leafs():
            for rev_num, rev_hash in revs:
                i = leaf.index(rev_num)
                if 0 <= i < len(leaf.path) and leaf.path[i] == rev_hash:
                    # return the max rev (latest=true)
                    yield leaf, leaf_rev_num
                    break  # check the next leaf

    def leafs(self):
        """All leafs in the tree. Those with the highest revision number and
        hash first.

        """
        for leaf in reversed(self):
            yield leaf, leaf.rev_num

    def winner_idx(self):
        """Returns the index of the winning leaf, i.e. the one with the highest
        rev that isn't deleted. If no such leafs exist, a deleted one suffices
        too.

        Assumption: leafs are sorted already. (Longest branches & highest rev
        hashes last)

        """
        for i in range(len(self) - 1, -1, -1):
            if self[i].doc_ptr is not None:
                return i  # we have a non-deleted winner
        return len(self) - 1  # no non-deleted ones exist

    def all_revs(self):
        """All revisions in the tree. Some can be yielded multiple times."""

        for leaf, leaf_rev_num in self.leafs():
            for i in range(len(leaf.path)):
                yield leaf, leaf_rev_num - i
