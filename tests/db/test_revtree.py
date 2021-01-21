from microcouch.db.revtree import RevisionTree, Branch


def validate_rev_tree(tree):
    for branch in tree:
        assert isinstance(branch, Branch)
        assert isinstance(branch.leaf_rev_num, int)
        assert branch.leaf_rev_num > 0
        assert isinstance(branch.path, list)
        assert all(isinstance(rev_hash, str) for rev_hash in branch.path)
        assert (branch.leaf_doc_ptr is None or
                isinstance(branch.leaf_doc_ptr, dict))
    assert sorted(tree, key=tree._by_max_rev) == tree


def test_new_branch():
    #        a
    #   b         c
    # d   e     f   g
    #
    tree = RevisionTree([
        Branch(3, ['d', 'b', 'a'], {}),
        Branch(3, ['e', 'b', 'a'], {}),
        Branch(3, ['f', 'c', 'a'], {}),
        Branch(3, ['g', 'c', 'a'], {}),
    ])
    assert [(b.leaf_rev_num, b.path[0]) for b in tree.branches()] == [
        (3, 'g'),
        (3, 'f'),
        (3, 'e'),
        (3, 'd'),
    ]
    tree.merge_with_path(doc_rev_num=3, doc_path=['h', 'c'], doc={})
    assert next(tree.branches()) == Branch(3, ['h', 'c', 'a'], {})


def test_order():
    tree = RevisionTree([])
    validate_rev_tree(tree)
    tree.merge_with_path(doc_rev_num=1, doc_path=['b'], doc={'x': 1})
    validate_rev_tree(tree)

    assert list(tree.branches()) == [
        Branch(1, ['b'], {'x': 1}),
    ]

    tree.merge_with_path(doc_rev_num=1, doc_path=['a'], doc={'x': 2})
    validate_rev_tree(tree)

    assert list(tree.branches()) == [
        Branch(1, ['b'], {'x': 1}),
        Branch(1, ['a'], {'x': 2}),
    ]

    tree.merge_with_path(doc_rev_num=1, doc_path=['c'], doc={'x': 3})
    validate_rev_tree(tree)

    assert list(tree.branches()) == [
        Branch(1, ['c'], {'x': 3}),
        Branch(1, ['b'], {'x': 1}),
        Branch(1, ['a'], {'x': 2}),
    ]


def test_new_winner():
    # 1-a 2-b 3-c
    #         3-x 4-y
    tree = RevisionTree([
        Branch(3, ['c', 'b', 'a'], {'name': 'c'}),
        Branch(4, ['y', 'x', 'b', 'a'], {'name': 'y'}),
    ])
    validate_rev_tree(tree)

    # insert 1-a 2-b 3-c 4-m 5-n
    tree.merge_with_path(doc_rev_num=5, doc_path=['n', 'm', 'c', 'b', 'a'],
                         doc={'name': 'n'})

    target = RevisionTree([
        Branch(4, ['y', 'x', 'b', 'a'], {'name': 'y'}),
        Branch(5, ['n', 'm', 'c', 'b', 'a'], {'name': 'n'}),
    ])

    validate_rev_tree(tree)
    assert tree == target


def test_revs_limit_basic():
    tree = RevisionTree([])

    tree.merge_with_path(doc_rev_num=2, doc_path=['b', 'a'], doc={},
                         revs_limit=1)
    validate_rev_tree(tree)

    assert tree == RevisionTree([
        Branch(2, ['b'], {}),
    ])


def test_revs_limit_advanced():
    # 1-a 2-b 3-c
    #     2-f 3-g
    tree = RevisionTree([
        Branch(3, ['c', 'b', 'a'], {}),
        Branch(3, ['g', 'f', 'a'], {}),
    ])
    validate_rev_tree(tree)

    # 1-a 2-f 3-g
    #
    # 2-b 3-c 4-d
    tree.merge_with_path(doc_rev_num=4, doc_path=['d', 'c', 'b', 'a'], doc={},
                         revs_limit=3)
    validate_rev_tree(tree)

    assert tree == RevisionTree([
        Branch(3, ['g', 'f', 'a'], {}),
        Branch(4, ['d', 'c', 'b'], {}),
    ])


def test_revs_limit_advanced2():
    # 1-a 2-e
    #     2-b 3-c
    # 4-d is added

    tree = RevisionTree([
        Branch(2, ['e', 'a'], {}),
        Branch(3, ['c', 'b', 'a'], {}),
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
        Branch(2, ['e', 'a'], {}),
        Branch(4, ['d', 'c'], {}),
    ])
