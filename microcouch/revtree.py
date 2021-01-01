from .utils import rev as _rev, parse_rev as _parse_rev

import bisect
import contextlib
import typing


class Leaf(typing.NamedTuple):
    rev_hash: str
    doc_ptr: typing.Optional[dict]  # None if deleted

    def with_child(self, max_revs, max_rev, tree):
        return Node(self.rev_hash, [tree]), [max_rev]


class Node(typing.NamedTuple):
    rev_hash: str
    children: list

    def with_child(self, max_revs, max_rev, tree):
        _insert_child(self.children, max_revs, max_rev, tree)
        return self, max_revs


class Root(typing.NamedTuple):
    start_rev_num: int
    tree: typing.Union[Node, Leaf]

    def with_child(self, *args, **kwargs):
        tree, max_revs = self.tree.with_child(*args, **kwargs)
        return Root(self.start_rev_num, tree), max_revs

    @property
    def rev_hash(self):
        return self.tree.rev_hash

    @property
    def children(self):
        return self.tree.children

    @property
    def doc_ptr(self):
        return self.tree.doc_ptr


class RevisionTree(typing.NamedTuple):
    """A revision tree like:

    '3-a' -> '4-b'
    '1-c' -> '2-e' -> '3-f'
          -> '2-d'

    can be represented using this class as:

    RevisionTree([
        Root(1, [
            Node("c", [
                Leaf("d", {}),
                Node("e", [
                    Leaf("f", {})
                ]),
            ]),
        ]),
        Root(3, [
            Node("a", [
                Leaf("b", {}),
            ]),
        ]),
    ])

    Note that the 'longest' branches (as they were before pruning) come last.
    If two branches are of equal length, they should be sorted by revision hash
    (from low -> high). This simplifies winner determination.

    """
    children: list

    def merge_with_path(self, doc_path, doc):
        ok, _, max_revs = self._merge_recur(doc_path, doc, self)
        if not ok:
            # merging was unsuccesful, insert the new doc directly into a new
            # root instead
            tree = self._construct_tree(doc_path['ids'], doc)
            root = Root(doc_path['start'] - len(doc_path['ids']) + 1, tree)

            revision = doc_path['start'], doc_path['ids'][0]
            i = bisect.bisect_left(max_revs, revision)
            self.children.insert(i, root)
        # TODO: prune

    def _merge_recur(self, doc_path, doc, node, rev_num=None):
        """Returns a (ok, node, max_revs) tuple. Ok is True
        when the doc is now inserted in the rev tree. Node is the (possibly
        replaced) current node. Max_revs is a list of, for each processed child
        node, a tuple of the revision number and revision hash of the 'maximum'
        branch.

        """
        ok, max_revs = False, []

        # we first recurse, then process, to make sure that we process starting
        # with the leafs and ending with the roots
        children = getattr(node, 'children', [])
        with contextlib.suppress(AttributeError):
            rev_num = node.start_rev_num  # root
        for i, child_node in enumerate(children):
            try:
                next_rev_num = rev_num + 1
            except TypeError:
                next_rev_num = None
            ok, new_child, child_max_revs = self._merge_recur(doc_path, doc,
                                                              child_node,
                                                              next_rev_num)
            if ok:
                del children[i]

                _insert_child(children, max_revs, child_max_revs[0], new_child)
                return ok, node, max_revs
            # no changes, so still sorted
            max_revs.append(child_max_revs[0])
        try:
            node.doc_ptr  # is leaf
        except AttributeError:
            pass
        else:
            # leaf node: max_revs will only contain 1 item
            rev_hash = node.rev_hash
            max_revs.append((rev_num, rev_hash))

        # base case: check if this is the insertion spot
        try:
            i = doc_path['start'] - rev_num
        except TypeError:
            i = -1
        else:
            rev_hash = node.rev_hash
        if 0 <= i < len(doc_path['ids']) and rev_hash == doc_path['ids'][i]:
            # now, based on doc_path and rev_num (through i), we construct
            # the 'new' part of the tree. This can consist of nothing (item
            # already in the tree), a single leaf node (all parent
            # revisions already known), but also a full tree containing
            # each rev in doc_path (e.g. when this item is completely new).
            if i > 0:
                tree = self._construct_tree(doc_path['ids'][:i], doc)
                max_rev = doc_path['start'], doc_path['ids'][0]
                node, max_revs = node.with_child(max_revs, max_rev, tree)
            ok = True
        return ok, node, max_revs

    def _construct_tree(self, rev_hashes, doc):
        tree = Leaf(rev_hashes[0], doc)
        for rev_hash in rev_hashes[1:]:
            tree = Node(rev_hash, [tree])
        return tree

    def find(self, revs, include_path):
        search_terms = {_parse_rev(rev) for rev in revs}
        for rev_num, path, node in self._walk(include_path):
            if (rev_num, node.rev_hash) in search_terms:
                # find all leaf branches for this doc (latest=true)
                yield from _tree_leafs(rev_num, path, node)

    def _walk_impl(self, include_path, tree_walk_func):
        for start_rev_num, tree in reversed(self.children):
            start_path = [] if include_path else None
            yield from tree_walk_func(start_rev_num, start_path, tree)

    def _walk(self, include_path=False):
        return self._walk_impl(include_path, tree_walk_func=_tree_walk)

    def leafs(self, include_path=False):
        return self._walk_impl(include_path, tree_walk_func=_tree_leafs)

    def winner(self):
        # assumption: leafs are sorted already (longest branches & highest rev
        # hashes first)
        deleted_winner = None
        for i, (rev_num, _, leaf) in enumerate(self.leafs()):
            if leaf.doc_ptr is not None:
                return rev_num, leaf  # we have a non-deleted winner
            if i == 0:
                deleted_winner = rev_num, leaf
        return deleted_winner  # no non-deleted ones exist

    def all_revs(self):
        return (_rev(num, rev_info) for num, _, rev_info in self._walk())

    # validation code
    def validate(self):
        for root in self.children:
            assert isinstance(root, Root)
            assert isinstance(root.tree, (Node, Leaf))
        for rev_num, _, node in self._walk():
            assert node.rev_hash
            if isinstance(node, Node):
                # contrary to a Leaf, a Node should have children.
                assert node.children
                def by_rev(node):
                    return self._get_first_leaf_rev(rev_num, node)
                sort_result = sorted(node.children, key=by_rev)
                # if sort_result != node.children:
                #     print([by_rev(n) for n in node.children])
                assert sort_result == node.children
            else:
                assert isinstance(node, Leaf)

    def _get_first_leaf_rev(self, rev_num, node):
        while not isinstance(node, Leaf):
            node = node.children[0]
            rev_num += 1
        return rev_num, node.rev_hash


def _tree_walk(start_rev_num, start_path, tree):
    stack = [(start_rev_num, start_path, tree)]
    while stack:
        rev_num, path, node = stack.pop()
        yield rev_num, path, node

        next_rev_num = rev_num + 1
        if path is None:
            next_path = None
        else:
            next_path = [(rev_num, node.rev_hash)] + path
        for child_tree in getattr(node, 'children', []):
            stack.append((next_rev_num, next_path, child_tree))


def _tree_leafs(start_rev_num, start_path, tree):
    walk = _tree_walk(start_rev_num, start_path, tree)
    for rev_num, path, node in walk:
        if isinstance(node, Leaf):
            yield rev_num, path, node


def _insert_child(children, max_revs, max_rev, new_child):
    j = bisect.bisect_left(max_revs, max_rev)
    max_revs.insert(j, max_rev)
    children.insert(j, new_child)
