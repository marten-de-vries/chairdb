from chairdb.db.revtree import RevisionTree, Branch


def validate_rev_tree(tree):
    for branch in tree:
        assert isinstance(branch, Branch)
        assert isinstance(branch.leaf_rev_num, int)
        assert branch.leaf_rev_num > 0
        assert isinstance(branch.path, tuple)
        assert all(isinstance(rev_hash, str) for rev_hash in branch.path)
        assert (branch.leaf_doc_ptr is None or
                isinstance(branch.leaf_doc_ptr, dict))
    assert sorted(tree, key=lambda b: b.leaf_rev_tuple) == tree


def merge_and_update(tree, doc_rev_num, doc_path, doc_ptr, revs_limit=1000):
    full_path, _, old_i = tree.merge_with_path(doc_rev_num, doc_path)
    assert full_path
    tree.update(doc_rev_num, full_path, doc_ptr, old_i, revs_limit)


def test_new_branch():
    #        a
    #   b         c
    # d   e     f   g
    #
    tree = RevisionTree([
        Branch(3, ('d', 'b', 'a'), {}),
        Branch(3, ('e', 'b', 'a'), {}),
        Branch(3, ('f', 'c', 'a'), {}),
        Branch(3, ('g', 'c', 'a'), {}),
    ])
    assert [branch.leaf_rev_tuple for branch in tree.branches()] == [
        (3, 'g'),
        (3, 'f'),
        (3, 'e'),
        (3, 'd'),
    ]
    merge_and_update(tree, doc_rev_num=3, doc_path=('h', 'c'), doc_ptr={})
    assert next(tree.branches()) == Branch(3, ('h', 'c', 'a'), {})


def test_order():
    tree = RevisionTree()
    validate_rev_tree(tree)
    merge_and_update(tree, doc_rev_num=1, doc_path=('b',), doc_ptr={'x': 1})
    validate_rev_tree(tree)

    assert list(tree.branches()) == [
        Branch(1, ('b',), {'x': 1}),
    ]

    merge_and_update(tree, doc_rev_num=1, doc_path=('a',), doc_ptr={'x': 2})
    validate_rev_tree(tree)

    assert list(tree.branches()) == [
        Branch(1, ('b',), {'x': 1}),
        Branch(1, ('a',), {'x': 2}),
    ]

    merge_and_update(tree, doc_rev_num=1, doc_path=('c',), doc_ptr={'x': 3})
    validate_rev_tree(tree)

    assert list(tree.branches()) == [
        Branch(1, ('c',), {'x': 3}),
        Branch(1, ('b',), {'x': 1}),
        Branch(1, ('a',), {'x': 2}),
    ]


def test_new_winner():
    # 1-a 2-b 3-c
    #         3-x 4-y
    tree = RevisionTree([
        Branch(3, ('c', 'b', 'a'), {'name': 'c'}),
        Branch(4, ('y', 'x', 'b', 'a'), {'name': 'y'}),
    ])
    validate_rev_tree(tree)

    # insert 1-a 2-b 3-c 4-m 5-n
    merge_and_update(tree, doc_rev_num=5, doc_path=('n', 'm', 'c', 'b', 'a'),
                     doc_ptr={'name': 'n'})

    target = RevisionTree([
        Branch(4, ('y', 'x', 'b', 'a'), {'name': 'y'}),
        Branch(5, ('n', 'm', 'c', 'b', 'a'), {'name': 'n'}),
    ])

    validate_rev_tree(tree)
    assert tree == target


def test_revs_limit_basic():
    tree = RevisionTree()

    merge_and_update(tree, doc_rev_num=2, doc_path=('b', 'a'), doc_ptr={},
                     revs_limit=1)
    validate_rev_tree(tree)

    assert tree == RevisionTree([
        Branch(2, ('b',), {}),
    ])


def test_revs_limit_advanced():
    # 1-a 2-b 3-c
    #     2-f 3-g
    tree = RevisionTree([
        Branch(3, ('c', 'b', 'a'), {}),
        Branch(3, ('g', 'f', 'a'), {}),
    ])
    validate_rev_tree(tree)

    # 1-a 2-f 3-g
    #
    # 2-b 3-c 4-d
    merge_and_update(tree, doc_rev_num=4, doc_path=('d', 'c', 'b', 'a'),
                     doc_ptr={}, revs_limit=3)
    validate_rev_tree(tree)

    assert tree == RevisionTree([
        Branch(3, ('g', 'f', 'a'), {}),
        Branch(4, ('d', 'c', 'b'), {}),
    ])


def test_revs_limit_advanced2():
    # 1-a 2-e
    #     2-b 3-c
    # 4-d is added

    tree = RevisionTree([
        Branch(2, ('e', 'a'), {}),
        Branch(3, ('c', 'b', 'a'), {}),
    ])
    validate_rev_tree(tree)
    merge_and_update(tree, doc_rev_num=4, doc_path=('d', 'c', 'b', 'a'),
                     doc_ptr={}, revs_limit=2)
    validate_rev_tree(tree)

    # 1-a 2-e
    # 3-c 4-d
    #
    # Note how 2b vanishes!
    assert tree == RevisionTree([
        Branch(2, ('e', 'a'), {}),
        Branch(4, ('d', 'c'), {}),
    ])


def test_diff():
    # test the 'lots of stemming' diffing case:
    tree = RevisionTree([
        Branch(5, ('b', 'a'), {}),
    ])
    assert tree.diff(2, ('c', 'd')) == (True, set())
