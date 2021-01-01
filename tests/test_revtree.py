from microcouch.revtree import RevisionTree, Root, Leaf

def test_order():
    tree = RevisionTree([])
    tree.validate()
    tree.merge_with_path({'start': 1, 'ids': ['b']}, doc={'x': 1})
    tree.validate()

    assert list(tree.leafs()) == [
        (1, None, Leaf('b', {'x': 1})),
    ]

    tree.merge_with_path({'start': 1, 'ids': ['a']}, doc={'x': 2})
    tree.validate()

    assert list(tree.leafs()) == [
        (1, None, Leaf('b', {'x': 1})),
        (1, None, Leaf('a', {'x': 2})),
    ]

    tree.merge_with_path({'start': 1, 'ids': ['c']}, doc={'x': 3})
    tree.validate()

    assert list(tree.leafs()) == [
        (1, None, Leaf('c', {'x': 3})),
        (1, None, Leaf('b', {'x': 1})),
        (1, None, Leaf('a', {'x': 2})),
    ]
