import bisect
import typing


class Branch(typing.NamedTuple):
    """A tree Branch, consisting of the leaf's revision number, it's parent
    revision hashes and a document (or None if deleted).

    """
    leaf_rev_num: int
    path: list
    leaf_doc_ptr: typing.Optional[dict]

    def index(self, rev_num):
        """Convert a revision number to a Branch.path index"""

        return self.leaf_rev_num - rev_num


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
    def __init__(self, children):
        super().__init__(children)

        # used to keep sorted by leaf's revision number and hash
        self._keys = [self._by_max_rev(branch) for branch in self]

    def _by_max_rev(self, branch):
        return branch.leaf_rev_num, branch.path[0]

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
            branch = self[i]
            # 1. check if already in tree. E.g.:
            #
            # branch.leaf_rev_num = 5
            # branch.path = ['e', 'd', 'c']
            #
            # doc_rev_num = 3
            # doc_path = ['c', 'b', 'a']
            j = branch.index(doc_rev_num)
            if 0 <= j < len(branch.path) and branch.path[j] == doc_path[0]:
                return  # it is. Done.

            # 2. extend branch if possible. E.g.:
            #
            # branch.leaf_rev_num = 3
            # branch.path = ['c', 'b', 'a']
            # doc_rev_num = 5
            # doc_path = ['e', 'd', 'c', 'b']
            k = doc_rev_num - branch.leaf_rev_num
            if 0 <= k < len(doc_path) and doc_path[k] == branch.path[0]:
                full_path = doc_path[:k] + branch.path
                del self[i]
                del self._keys[i]
                self._insert_branch(doc_rev_num, full_path, doc, revs_limit)
                return  # it is. Done.

        # otherwise insert as a new leaf branch:
        self._insert_as_new_branch(doc_rev_num, doc_path, doc, revs_limit)

    def _insert_as_new_branch(self, doc_rev_num, doc_path, doc, revs_limit):
        for branch in self.branches():
            # 3. try to find common history
            start_leaf_rev_num = branch.leaf_rev_num + 1 - len(branch.path)
            start_doc_rev_num = doc_rev_num + 1 - len(doc_path)
            maybe_common_rev_num = max(start_leaf_rev_num, start_doc_rev_num)

            branch_i = branch.index(maybe_common_rev_num)
            doc_i = doc_rev_num - maybe_common_rev_num

            common_rev = (
                0 <= branch_i < len(branch.path) and
                0 <= doc_i < len(doc_path) and
                branch.path[branch_i] == doc_path[doc_i]
            )
            if common_rev:
                # success, combine both halves into a 'full_path'
                full_path = doc_path[:doc_i] + branch.path[branch_i:]
                break
        else:
            # 4. a new branch without shared history
            full_path = doc_path

        self._insert_branch(doc_rev_num, full_path, doc, revs_limit)

    def _insert_branch(self, doc_rev_num, full_path, doc, revs_limit):
        # stem using revs_limit
        assert revs_limit > 0
        del full_path[revs_limit:]

        # actual insertion using bisection
        branch = Branch(doc_rev_num, full_path, doc)
        key = self._by_max_rev(branch)
        i = bisect.bisect(self._keys, key)
        self._keys.insert(i, key)
        self.insert(i, branch)

    def find(self, revs):
        """For each revision number in 'revs', find the branch pointed to by
        said revisions.

        """
        for rev_num, rev_hash in revs:
            for branch in self.branches():
                i = branch.index(rev_num)
                if 0 <= i < len(branch.path) and branch.path[i] == rev_hash:
                    yield branch
                    break  # check the next requested rev

    def branches(self):
        """All branches in the tree. Those with the highest revision number and
        hash first.

        """
        for branch in reversed(self):
            yield branch

    def winner_idx(self):
        """Returns the index of the winning branch, i.e. the one with the
        highest leaf rev that isn't deleted. If no such branches exist, a
        deleted one suffices too.

        Assumption: branches are sorted already. (Longest branches & highest
        rev hashes last)

        """
        for i in range(len(self) - 1, -1, -1):
            if self[i].leaf_doc_ptr is not None:
                return i  # we have a non-deleted winner
        return len(self) - 1  # no non-deleted ones exist

    def all_revs(self):
        """All revisions in the tree. Some can be yielded multiple times."""

        for branch in self.branches():
            for i in range(len(branch.path)):
                yield branch, branch.leaf_rev_num - i