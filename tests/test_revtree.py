from microcouch.revtree import (RevisionTree, Root, Node, Leaf, preorder_only,
                                track_max_revs, validate_rev_tree)


def test_walk():
    #        1
    #   2         3
    # 4   5     6   7
    #
    tree = RevisionTree([
        Root(1, Node('1', [
            Node('2', [
                Leaf('4', {}),
                Leaf('5', {}),
            ]),
            Node('3', [
                Leaf('6', {}),
                Leaf('7', {}),
            ]),
        ])),
    ])

    walk1 = [(rev_num, node.rev_hash)
             for node, rev_num, _ in preorder_only(tree._walk())]
    assert walk1 == [
        (1, '1'),
        (2, '3'),
        (3, '7'),
        (3, '6'),
        (2, '2'),
        (3, '5'),
        (3, '4'),
    ]

    walk2 = [(rev_num, node.rev_hash, preorder)
             for node, preorder, rev_num, _ in tree._walk()]
    assert walk2 == [
        (1, '1', True),
        (2, '3', True),
        (3, '7', True),
        (3, '7', False),
        (3, '6', True),
        (3, '6', False),
        (2, '3', False),
        (2, '2', True),
        (3, '5', True),
        (3, '5', False),
        (3, '4', True),
        (3, '4', False),
        (2, '2', False),
        (1, '1', False),
    ]

    max_revs_stack = [[]]
    walk3 = track_max_revs(tree._walk(), max_revs_stack)
    assert max_revs_stack == [[]]
    # copy on each iteration, because max_revs_stack is modified in-place
    walk3resp = [(rev_num, node.rev_hash, [s[:] for s in max_revs_stack])
                 for node, rev_num, _ in walk3]
    assert walk3resp == [
        (3, '7', [[], [], [], [(3, '7')]]),
        (3, '6', [[], [], [(3, '7')], [(3, '6')]]),
        (2, '3', [[], [], [(3, '7'), (3, '6')]]),
        (3, '5', [[], [(3, '7')], [], [(3, '5')]]),
        (3, '4', [[], [(3, '7')], [(3, '5')], [(3, '4')]]),
        (2, '2', [[], [(3, '7')], [(3, '5'), (3, '4')]]),
        (1, '1', [[], [(3, '7'), (3, '5')]])
    ]
    # check at the end
    assert max_revs_stack == [[(3, '7')]]

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

    print('---')
    print(tree.as_tree())
    print('---')
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
