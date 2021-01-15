from microcouch.revtree import (RevisionTree, Leaf, by_max_rev)


def validate_rev_tree(tree):
    for leaf in tree:
        assert isinstance(leaf, Leaf)
        assert isinstance(leaf.rev_num, int)
        assert leaf.rev_num > 0
        assert isinstance(leaf.path, list)
        assert all(isinstance(a, str) for a in leaf.path)
        assert leaf.doc_ptr is None or isinstance(leaf.doc_ptr, dict)
    assert sorted(tree, key=by_max_rev) == tree


def test_new_branch():
    #        a
    #   b         c
    # d   e     f   g
    #
    tree = RevisionTree([
        Leaf(3, ['d', 'b', 'a'], {}),
        Leaf(3, ['e', 'b', 'a'], {}),
        Leaf(3, ['f', 'c', 'a'], {}),
        Leaf(3, ['g', 'c', 'a'], {}),
    ])
    assert [(num, leaf.path[0]) for leaf, num in tree.leafs()] == [
        (3, 'g'),
        (3, 'f'),
        (3, 'e'),
        (3, 'd'),
    ]
    tree.merge_with_path(doc_rev_num=3, doc_path=['h', 'c'], doc={})
    assert next(tree.leafs()) == (Leaf(3, ['h', 'c', 'a'], {}), 3)


def test_order():
    tree = RevisionTree([])
    validate_rev_tree(tree)
    tree.merge_with_path(doc_rev_num=1, doc_path=['b'], doc={'x': 1})
    validate_rev_tree(tree)

    assert list(tree.leafs()) == [
        (Leaf(1, ['b'], {'x': 1}), 1),
    ]

    tree.merge_with_path(doc_rev_num=1, doc_path=['a'], doc={'x': 2})
    validate_rev_tree(tree)

    assert list(tree.leafs()) == [
        (Leaf(1, ['b'], {'x': 1}), 1),
        (Leaf(1, ['a'], {'x': 2}), 1),
    ]

    tree.merge_with_path(doc_rev_num=1, doc_path=['c'], doc={'x': 3})
    validate_rev_tree(tree)

    assert list(tree.leafs()) == [
        (Leaf(1, ['c'], {'x': 3}), 1),
        (Leaf(1, ['b'], {'x': 1}), 1),
        (Leaf(1, ['a'], {'x': 2}), 1),
    ]


def test_new_winner():
    # 1-a 2-b 3-c
    #         3-x 4-y
    tree = RevisionTree([
        Leaf(3, ['c', 'b', 'a'], {'name': 'c'}),
        Leaf(4, ['y', 'x', 'b', 'a'], {'name': 'y'}),
    ])
    validate_rev_tree(tree)

    # insert 1-a 2-b 3-c 4-m 5-n
    tree.merge_with_path(doc_rev_num=5, doc_path=['n', 'm', 'c', 'b', 'a'],
                         doc={'name': 'n'})

    target = RevisionTree([
        Leaf(4, ['y', 'x', 'b', 'a'], {'name': 'y'}),
        Leaf(5, ['n', 'm', 'c', 'b', 'a'], {'name': 'n'}),
    ])

    validate_rev_tree(tree)
    assert tree == target


def test_revs_limit_basic():
    tree = RevisionTree([])

    tree.merge_with_path(doc_rev_num=2, doc_path=['b', 'a'], doc={},
                         revs_limit=1)
    validate_rev_tree(tree)

    assert tree == RevisionTree([
        Leaf(2, ['b'], {}),
    ])


def test_revs_limit_advanced():
    # 1-a 2-b 3-c
    #     2-f 3-g
    tree = RevisionTree([
        Leaf(3, ['c', 'b', 'a'], {}),
        Leaf(3, ['g', 'f', 'a'], {}),
    ])
    validate_rev_tree(tree)

    # 1-a 2-f 3-g
    #
    # 2-b 3-c 4-d
    tree.merge_with_path(doc_rev_num=4, doc_path=['d', 'c', 'b', 'a'], doc={},
                         revs_limit=3)
    validate_rev_tree(tree)

    assert tree == RevisionTree([
        Leaf(3, ['g', 'f', 'a'], {}),
        Leaf(4, ['d', 'c', 'b'], {}),
    ])


def test_revs_limit_advanced2():
    # 1-a 2-e
    #     2-b 3-c
    # 4-d is added

    tree = RevisionTree([
        Leaf(2, ['e', 'a'], {}),
        Leaf(3, ['c', 'b', 'a'], {}),
    ])
    validate_rev_tree(tree)
    tree.merge_with_path(doc_rev_num=4, doc_path=['d', 'c', 'b', 'a'], doc={},
                         revs_limit=2)
    validate_rev_tree(tree)

    # 1-a 2-e
    # 3-c 4-d
    #
    # Note how 2b vanishes!
    assert tree == RevisionTree([
        Leaf(2, ['e', 'a'], {}),
        Leaf(4, ['d', 'c'], {}),
    ])
