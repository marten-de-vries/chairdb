# TODO: clean up & make nice.
# TODO: revs limit (+ update documentation)

import bisect
import collections
import functools
import typing


class Leaf(typing.NamedTuple):
    """A leaf node represents a last version of a document. Note that it's not
    automatically the winner: it could be the junior member of a conflict.

    """
    rev_hash: str
    doc_ptr: typing.Optional[dict]  # None if deleted

    def with_child(self, max_revs, max_rev, tree):
        # this essentially handles auto-compaction
        return Node(self.rev_hash, [tree]), [max_rev]


class Node(typing.NamedTuple):
    """A past revision of one (or more) of the leafs of the document. For child
    order, see RevisionTree.

    """
    rev_hash: str
    children: list

    def with_child(self, max_revs, max_rev, tree):
        insert_child(self.children, max_revs, max_rev, tree)
        return self, max_revs


class Root(typing.NamedTuple):
    """A start of a tree. There can be multiple starts if the initial revisions
    are conflicted or through revision pruning.

    For many purposes, a Root can be treated as the Leaf or Node object it
    wraps due to the @property definitions in this class which function as a
    proxy.

    """
    start_rev_num: int
    tree: typing.Union[Node, Leaf]


class RevisionTree(typing.NamedTuple):
    """A revision tree like:

    '3-a' -> '4-b'

    '1-c' -> '2-e' -> '3-f'
          -> '2-d'

    can be represented using this class as:

    RevisionTree([
        Root(1, Node("c", [
            Leaf("d", {}),
            Node("e", [
                Leaf("f", {})
            ]),
        ])),
        Root(3, Node("a", [
            Leaf("b", {}),
        ])),
    ])

    Note that the 'longest' branches (as they were before pruning) come last.
    If two branches are of equal length, they should be sorted by revision hash
    (from low -> high). This simplifies winner determination.

    """
    children: list

    def with_child(self, max_revs, max_rev, tree):
        insert_child(self.children, max_revs, max_rev, tree)
        return self, max_revs

    def merge_with_path(self, doc_rev_num, doc_path, doc):
        """Merges a document into the revision tree, storing 'doc' into a new
        leaf node (assuming the location pointed at by 'rev_num' and 'path'
        would in fact be a leaf node, which is not the case if a document has
        already been replaced by a newer version). 'rev_num' is the revision
        number of the document. 'path' is a list of revision hashes. The first
        hash is the last (i.e. current) revision of the document, while the
        last one is its earliest known parent revision.

        """
        max_revs_stack = [[]]
        walk = track_max_revs(self._walk(include_path=True), max_revs_stack)

        # this dummy value is only used if the database is completely empty
        for node, rev_num, path in walk:
            i = (doc_rev_num - rev_num)
            if 0 <= i < len(doc_path) and node.rev_hash == doc_path[i]:
                # it is the insertion spot!
                break
        else:
            # merging was unsuccesful, insert the new doc directly into a new
            # root instead
            start_rev_num = doc_rev_num - len(doc_path) + 1
            max_rev = doc_rev_num, doc_path[0]

            tree = construct_tree(doc_path, doc)
            root = Root(start_rev_num, tree)
            max_revs = max_revs_stack.pop()
            max_revs.reverse()
            self.with_child(max_revs, max_rev, root)
            return
        if i > 0:
            # item not already in the tree
            #
            # now, based on doc_path and rev_num (through i), we construct
            # the 'new' part of the tree. This can consist of a single leaf
            # node (all parent revisions already known), but also a full
            # tree containing (almost) each rev in doc_path.
            tree = construct_tree(doc_path[:i], doc)
            max_rev = doc_rev_num, doc_path[0]
            max_revs = max_revs_stack.pop()
            max_revs.reverse()
            node, max_revs = node.with_child(max_revs, max_rev, tree)

            both = zip(path, reversed(max_revs_stack))
            for (parent_node, i), parent_max_revs in both:
                parent_max_revs.reverse()
                old_child = parent_node.children.pop(i)
                if isinstance(old_child, Root):
                    # re-wrap
                    node = Root(old_child.start_rev_num, node)
                node, max_revs = parent_node.with_child(parent_max_revs,
                                                        max_revs[-1], node)

    def find(self, revs, include_path):
        """For each revision number in 'revs', find the leaf documents of the
        branches pointed to by said revisions.

        """
        # first find all the nodes that match the requested revisions
        for node, rev_num, path in preorder_only(self._walk(include_path)):
            if (rev_num, node.rev_hash) in revs:
                # find all leaf branches for this doc (latest=true)
                yield from leafs_only(walk(node, rev_num, path))

    def _walk(self, include_path=False):
        indexes = range(len(self.children))
        for i, root in zip(reversed(indexes), reversed(self.children)):
            path = collections.deque([[self, i]]) if include_path else None
            yield from walk(root.tree, root.start_rev_num, path)

    def leafs(self, include_path=False):
        return leafs_only(self._walk(include_path))

    def winner(self):
        # assumption: leafs are sorted already (longest branches & highest rev
        # hashes last, which means they are encountered *first* when using the
        # walk functions. They are defined that way.)
        deleted_winner = None
        for i, (leaf, *info) in enumerate(self.leafs(include_path=True)):
            if leaf.doc_ptr is not None:
                return leaf, *info  # we have a non-deleted winner
            if i == 0:
                deleted_winner = leaf, *info
        return deleted_winner  # no non-deleted ones exist

    def all_revs(self):
        for node, rev_num, _ in preorder_only(self._walk()):
            yield node, rev_num


def walk(start_node, rev_num, path):
    base = with_rev_num(tree_walk(start_node), rev_num)
    if path is None:
        return with_dummy(base)  # just put 'None' in the place of 'path'
    else:
        return with_path(base, path)


def tree_walk(start_node):
    stack = [(start_node, True)]
    while stack:
        node, preorder = stack.pop()
        yield node, preorder

        if preorder:
            # visit again after the children by pushing it to the stack first
            stack.append((node, False))
            for i, child_tree in enumerate(getattr(node, 'children', [])):
                stack.append((child_tree, True))


def with_rev_num(walk, rev_num):
    # start rev_num: Root.rev_num
    for node, preorder, *extra in walk:
        if not preorder:
            rev_num -= 1
        yield node, preorder, *extra, rev_num
        if preorder:
            rev_num += 1


def leafs_only(walk):
    for node, *extra in preorder_only(walk):
        if isinstance(node, Leaf):
            yield node, *extra


def preorder_only(walk):
    for node, preorder, *extra in walk:
        if preorder:
            yield node, *extra


def track_max_revs(walk, max_revs_stack):
    # similar to other methods, but max_revs_stack isn't yielded all the time.
    # The reason is that it's value is also meaningful *before* any iteration
    # has occurred, and after iteration has finished. Yielding would be
    # confusing. Requires rev_num as first 'walk wrapper'.
    #
    # start max_revs_stack: [[]]
    for node, preorder, rev_num, *extra in walk:
        if preorder:
            max_revs_stack.append([])
            if isinstance(node, Leaf):
                max_revs_stack[-1].append((rev_num, node.rev_hash))
        if not preorder:
            # only yield during postorder: the max_revs_stack is meaningless
            # during preorder.
            yield node, rev_num, *extra

            current_max_rev = max_revs_stack.pop()[0]
            max_revs_stack[-1].append(current_max_rev)


def with_dummy(walk):
    for row in walk:
        yield *row, None


def with_path(walk, path):
    # path should be a collections.deque
    #
    # start path: [[tree, 0]]
    for node, preorder, *extra in walk:
        if not (isinstance(node, Leaf) or preorder):
            del path[0]

        yield node, preorder, *extra, path
        if not isinstance(node, Leaf) and preorder:
            path.appendleft([node, len(node.children) - 1])

        if not preorder:
            # increase child index (we're in a parent node again)
            path[0][1] -= 1


def construct_tree(rev_hashes, doc):
    """Construct a linear tree (with always only a single child node)"""

    tree = Leaf(rev_hashes[0], doc)
    for rev_hash in rev_hashes[1:]:
        tree = Node(rev_hash, [tree])
    return tree


def insert_child(children, max_revs, max_rev, new_child):
    # 'max_revs' might be shorter than 'children' because we don't care about
    # revisions that were (before insertion) already less than the branch
    # currently being updated.
    i = bisect.bisect_left(max_revs, max_rev)
    # ... which only works if we adjust the index to be the same 'from the end'
    child_i = i - len(max_revs) + len(children)
    max_revs.insert(i, max_rev)
    children.insert(child_i, new_child)


# validation stuff:


def validate_rev_tree(tree):
    for root in tree.children:
        assert isinstance(root, Root)
        assert root.start_rev_num > 0
        validate_node(root.start_rev_num, root.tree)


def validate_node(rev_num, node):
    assert isinstance(node.rev_hash, str)
    if isinstance(node, Node):
        assert node.children
        by_max_rev = functools.partial(by_last_rev_recur, rev_num)
        assert sorted(node.children, key=by_max_rev) == node.children

        for subnode in node.children:
            validate_node(rev_num + 1, subnode)
    else:
        assert isinstance(node, Leaf)
        assert node.doc_ptr is None or isinstance(node.doc_ptr, dict)


def by_last_rev_recur(rev_num, node):
    if isinstance(node, Leaf):
        return rev_num, node.rev_hash
    return by_last_rev_recur(rev_num + 1, node.children[-1])


# debugging stuff:

def as_tree_node(node, rev_num):
    if isinstance(node, Leaf):
        yield f'{rev_num}-{node.rev_hash}: {node.doc_ptr}'
    else:
        this = f'{rev_num}-{node.rev_hash} '
        first = True
        for child in node.children:
            for line in as_tree_node(child, rev_num + 1):
                if first:
                    first = False
                    yield this + line
                else:
                    yield len(this) * ' ' + line


def as_tree_root(root):
    return '\n'.join(as_tree_node(root.tree, root.start_rev_num))


def as_tree(rev_tree):
    return '\n\n'.join(as_tree_root(root) for root in rev_tree.children)
