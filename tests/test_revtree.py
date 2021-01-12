from microcouch.revtree import (RevisionTree, Root, Node, Leaf, preorder_only,
                                track_max_revs, validate_rev_tree, as_tree)


#        a
#   b         c
# d   e     f   g
#
TREE = RevisionTree([
    Root(1, Node('a', [
        Node('b', [
            Leaf('d', {}),
            Leaf('e', {}),
        ]),
        Node('c', [
            Leaf('f', {}),
            Leaf('g', {}),
        ]),
    ])),
])


def test_debug():
    assert as_tree(TREE) == """1-a 2-b 3-d: {}
        3-e: {}
    2-c 3-f: {}
        3-g: {}"""


def test_walk():
    walk1 = [(rev_num, node.rev_hash)
             for node, rev_num, _ in preorder_only(TREE._walk())]
    assert walk1 == [
        (1, 'a'),
        (2, 'c'),
        (3, 'g'),
        (3, 'f'),
        (2, 'b'),
        (3, 'e'),
        (3, 'd'),
    ]

    walk2 = [(rev_num, node.rev_hash, preorder)
             for node, preorder, rev_num, _ in TREE._walk()]
    assert walk2 == [
        (1, 'a', True),
        (2, 'c', True),
        (3, 'g', True),
        (3, 'g', False),
        (3, 'f', True),
        (3, 'f', False),
        (2, 'c', False),
        (2, 'b', True),
        (3, 'e', True),
        (3, 'e', False),
        (3, 'd', True),
        (3, 'd', False),
        (2, 'b', False),
        (1, 'a', False),
    ]

    max_revs_stack = [[]]
    walk3 = track_max_revs(TREE._walk(), max_revs_stack)
    assert max_revs_stack == [[]]
    # copy on each iteration, because max_revs_stack is modified in-place
    walk3resp = [(rev_num, node.rev_hash, [s[:] for s in max_revs_stack])
                 for node, rev_num, _ in walk3]
    assert walk3resp == [
        (3, 'g', [[], [], [], [(3, 'g')]]),
        (3, 'f', [[], [], [(3, 'g')], [(3, 'f')]]),
        (2, 'c', [[], [], [(3, 'g'), (3, 'f')]]),
        (3, 'e', [[], [(3, 'g')], [], [(3, 'e')]]),
        (3, 'd', [[], [(3, 'g')], [(3, 'e')], [(3, 'd')]]),
        (2, 'b', [[], [(3, 'g')], [(3, 'e'), (3, 'd')]]),
        (1, 'a', [[], [(3, 'g'), (3, 'e')]])
    ]
    # check at the end
    assert max_revs_stack == [[(3, 'g')]]


def test_order():
    tree = RevisionTree([])
    validate_rev_tree(tree)
    tree.merge_with_path(doc_rev_num=1, doc_path=['b'], doc={'x': 1})
    validate_rev_tree(tree)

    assert list(tree.leafs()) == [
        (Leaf('b', {'x': 1}), 1, None),
    ]

    tree.merge_with_path(doc_rev_num=1, doc_path=['a'], doc={'x': 2})
    validate_rev_tree(tree)

    assert list(tree.leafs()) == [
        (Leaf('b', {'x': 1}), 1, None),
        (Leaf('a', {'x': 2}), 1, None),
    ]

    tree.merge_with_path(doc_rev_num=1, doc_path=['c'], doc={'x': 3})
    validate_rev_tree(tree)

    assert list(tree.leafs()) == [
        (Leaf('c', {'x': 3}), 1, None),
        (Leaf('b', {'x': 1}), 1, None),
        (Leaf('a', {'x': 2}), 1, None),
    ]


def test_new_winner():
    # 1-a 2-b 3-c
    #         3-x 4-y
    tree = RevisionTree([
        Root(1, Node('a', [
            Node('b', [
                Leaf('c', {'name': 'c'}),
                Node('x', [
                    Leaf('y', {'name': 'y'}),
                ]),
            ]),
        ]))
    ])
    validate_rev_tree(tree)

    # insert 1-a 2-b 3-c 4-m 5-n
    tree.merge_with_path(doc_rev_num=5, doc_path=['n', 'm', 'c', 'b', 'a'],
                         doc={'name': 'n'})

    target = RevisionTree([
        Root(1, Node('a', [
            Node('b', [
                Node('x', [
                    Leaf('y', {'name': 'y'}),
                ]),
                Node('c', [
                    Node('m', [
                        Leaf('n', {'name': 'n'})
                    ])
                ]),
            ]),
        ]))
    ])

    validate_rev_tree(tree)
    assert tree == target
