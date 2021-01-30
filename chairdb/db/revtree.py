import bisect
import typing


class Branch(typing.NamedTuple):
    """A tree Branch, consisting of the leaf's revision number, it's own and
    ancestor revision hashes and a document (or None if deleted).

    """
    leaf_rev_num: int
    path: tuple
    leaf_doc_ptr: typing.Optional[typing.Any]

    def index(self, rev_num):
        """Convert a revision number to a Branch.path index"""

        return self.leaf_rev_num - rev_num

    def contains(self, rev_num, rev_hash):
        i = self.index(rev_num)
        return 0 <= i < len(self.path) and self.path[i] == rev_hash

    @property
    def leaf_rev_tuple(self):
        return self.leaf_rev_num, self.path[0]


class RevisionTree(list):
    """A revision tree like:

    '3-a' -> '4-b'

    '1-c' -> '2-e' -> '3-f'
          -> '2-d'

    can be represented using this class as:

    RevisionTree([
        Branch(2, ['d', 'c'], {}),
        Branch(3, ['f', 'e', 'c'], {}),
        Branch(4, ['b', 'a'], {}),
    ])

    Note that the 'longest' branches (as they were before pruning) come last.
    If two branches are of equal length, they should be sorted by revision hash
    (from low -> high). This simplifies winner determination.

    """
    def __init__(self, branches=[]):
        super().__init__(branches)

        # used to keep the tree sorted by leaf's revision number and hash
        self._keys = [branch.leaf_rev_tuple for branch in self]

    def merge_with_path(self, doc_rev_num, doc_path):
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
            branch = self[i]
            # 1. check if already in tree. E.g.:
            #
            # branch.leaf_rev_num = 5
            # branch.path = ('e', 'd', 'c')
            #
            # doc_rev_num = 3
            # doc_path = ('c', 'b', 'a')
            if branch.contains(doc_rev_num, doc_path[0]):
                # it is. Done. The new doc can be removed
                return None, None

            # 2. extend branch if possible. E.g.:
            #
            # branch.leaf_rev_num = 3
            # branch.path = ('c', 'b', 'a')
            # doc_rev_num = 5
            # doc_path = ('e', 'd', 'c', 'b')
            k = doc_rev_num - branch.leaf_rev_num
            if 0 <= k < len(doc_path) and doc_path[k] == branch.path[0]:
                # it is. Done. The old doc can be removed.
                return doc_path[:k] + branch.path, i

        # otherwise insert as a new leaf branch:
        return self._insert_as_new_branch(doc_rev_num, doc_path)

    def _insert_as_new_branch(self, doc_rev_num, doc_path):
        for branch in self.branches():
            # 3. try to find common history
            start_branch_rev_num = branch.leaf_rev_num + 1 - len(branch.path)
            start_doc_rev_num = doc_rev_num + 1 - len(doc_path)
            maybe_common_rev_num = max(start_branch_rev_num, start_doc_rev_num)

            branch_i = branch.index(maybe_common_rev_num)
            doc_i = doc_rev_num - maybe_common_rev_num

            common_rev = (
                0 <= branch_i < len(branch.path) and
                0 <= doc_i < len(doc_path) and
                branch.path[branch_i] == doc_path[doc_i]
            )
            if common_rev:
                # success, combine both halves into a 'full_path'
                return doc_path[:doc_i] + branch.path[branch_i:], None
        # 4. a new branch without shared history
        return doc_path, None

    def update(self, rev_num, path, ptr, old_index=None, revs_limit=1000):
        if old_index is not None:
            # replace by removing the old branch first
            del self[old_index]
            del self._keys[old_index]

        # stem using revs_limit
        assert revs_limit > 0, "invalid revs limit"
        path = path[:revs_limit]

        new_branch = Branch(rev_num, path, ptr)
        # actual insertion using bisection
        key = new_branch.leaf_rev_tuple
        i = bisect.bisect(self._keys, key)
        self._keys.insert(i, key)
        self.insert(i, new_branch)

    def find(self, rev_num, rev_hash):
        """Find the branches in which the revision specified by the arguments
        occurs.

        """
        for branch in self.branches():
            if branch.contains(rev_num, rev_hash):
                yield branch

    def diff(self, rev_num, rev_hash):
        """Takes a revision (rev_num, rev_hash) as its input. Returns a
        (is_missing, possible_ancestors) tuple. ``is_missing`` is a bool that's
        False when the revision is in the tree. If it's True,
        ``possible_ancestors`` will be a set of branches that could
        theoretically be extended to include the revision.

        """
        possible_ancestors = set()
        for branch in self.branches():
            if branch.contains(rev_num, rev_hash):
                return False, None
            elif rev_num > branch.leaf_rev_num:
                possible_ancestors.add(branch.leaf_rev_tuple)
        return True, possible_ancestors

    def branches(self):
        """All branches in the tree. Those with the highest revision number and
        hash first.

        """
        return reversed(self)

    def winner(self):
        """Returns the winning branch, i.e. the one with the highest leaf rev
        that isn't deleted. If no such branches exist, a deleted one suffices
        too.

        Assumption: branches are sorted already. (Longest branches & highest
        rev hashes last)

        """
        best_deleted_branch = None
        for i, branch in enumerate(self.branches()):
            if branch.leaf_doc_ptr is not None:
                return branch  # best non-deleted branch
            best_deleted_branch = best_deleted_branch or branch
        return best_deleted_branch
