from microcouch.revtree import RevisionTree, Root, Leaf, validate_rev_tree

def test_order():
    tree = RevisionTree([])
    validate_rev_tree(tree)
    tree.merge_with_path(rev_num=1, path=['b'], doc={'x': 1})
    validate_rev_tree(tree)

    assert list(tree.leafs()) == [
        (1, None, Leaf('b', {'x': 1})),
    ]

    tree.merge_with_path(rev_num=1, path=['a'], doc={'x': 2})
    validate_rev_tree(tree)

    assert list(tree.leafs()) == [
        (1, None, Leaf('b', {'x': 1})),
        (1, None, Leaf('a', {'x': 2})),
    ]

    tree.merge_with_path(rev_num=1, path=['c'], doc={'x': 3})
    validate_rev_tree(tree)

    assert list(tree.leafs()) == [
        (1, None, Leaf('c', {'x': 3})),
        (1, None, Leaf('b', {'x': 1})),
        (1, None, Leaf('a', {'x': 2})),
    ]
