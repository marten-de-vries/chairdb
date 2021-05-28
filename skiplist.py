import math
import random


class SkipList:
    def __init__(self):
        # example from: https://en.wikipedia.org/wiki/Skip_list
        self.store = {
            '_head': [(1, 0), (1, 0), (1, 0), (1, None)],
            1: [(None, 10), (4, 3), (3, 2), (2, 'a')],
            2: [(3, 'b')],
            3: [(4, 1), (4, 'c')],
            4: [(6, 2), (6, 2), (5, 'd')],
            5: [(6, 'e')],
            6: [(None, 5), (9, 3), (7, 'f')],
            7: [(8, 'g')],
            8: [(9, 'h')],
            9: [(None, 2), (10, 'i')],
            10: [(None, 'j')],
        }

    def lookup(self, target_key, start_key='_head', inclusive=False):
        key = start_key
        while key != target_key:
            node = self.store[key]
            for i, (next_key, value) in enumerate(node):
                # When keys are numeric, you can think of None as a very high
                # (e.g. infinite) number.
                if target_key is None or (next_key is not None and
                                          target_key >= next_key):
                    yield key, next_key, value, node, i
                    key = next_key
                    break  # the inner loop, because we just moved right
            else:
                # stop the outer loop, we went as far 'down' and 'right' as
                # possible
                break
        if key is not None and (key != target_key or inclusive):
            # make the minimal step (i.e. the bottommost one)
            node = self.store[key]
            i = len(node) - 1
            next_key, value = node[i]
            yield key, next_key, value, node, i

    def query(self, start_key, end_key):
        start = '_head'
        if start_key is not None:
            for _, start, *_ in self.lookup(start_key, start):
                pass  # start is an actual key (unlike, possibly, start_key)
        lookup = self.lookup(end_key, start_key=start, inclusive=True)
        for i, (_, key, value, node, node_i) in enumerate(lookup):
            v = self._reduce_value(key, value, len(node) - node_i)
            if i == 0:
                result = v
            else:
                result = self.rereduce(result, v)
        return result

    def _reduce_value(self, key, value, level):
        if level == 1:
            return self.reduce(key, value)
        else:
            return value

    def reduce(self, key, value):
        return 1

    def rereduce(self, *values):
        return sum(values)

    def __getitem__(self, target_key):
        *_, (key, _, _, node, _) = self.lookup(target_key, inclusive=True)
        if key != target_key:
            raise KeyError(target_key)
        [*_, (_, value)] = node
        return value

    def __setitem__(self, key, value):
        # repeated coin tosses (probability 1/2, hence log2)
        height = math.ceil(-math.log2(random.random()))

        path = list(self.lookup(key, inclusive=True))
        for *args, node, i in reversed(path):
            print(*args, len(node) - i)
            print(node)
    #     new_level_count = height - len(self.head)  # can be negative
    #     new_node = [None] * new_level_count
    #
    #     node = self.head
    #     while True:
    #         for i, (next_key, reduce_value) in enumerate(node):
    #             if next_key is not None and key >= next_key:
    #                 current_key = next_key
    #                 node = self.store[next_key]
    #                 break
    #             if i < height:
    #                 self.store[current_key][0][i] = key
    #                 new_node.append(next_key)
    #         else:
    #             break  # the outer loop
    #     self.store[key] = value, new_node
    #     self.head += [key] * new_level_count


if __name__ == '__main__':
    skip_list = SkipList()

    for i in range(1, 11):
        print(skip_list[i])

    def lookup(*args, **kwargs):
        for *args, node, i in skip_list.lookup(*args, **kwargs):
            yield *args, len(node) - i  # i.e. the level, 1 = bottom list
    assert list(lookup(2)) == [
        ('_head', 1, 0, 4),
        (1, 2, 'a', 1),
    ]
    assert list(lookup(1.5)) == [
        ('_head', 1, 0, 4),
        (1, 2, 'a', 1),
    ]
    assert list(lookup(None)) == [
        ('_head', 1, 0, 4),
        (1, None, 10, 4),
    ]
    assert list(lookup(8)) == [
        ('_head', 1, 0, 4),
        (1, 4, 3, 3),
        (4, 6, 2, 3),
        (6, 7, 'f', 1),
        (7, 8, 'g', 1),
    ]
    assert list(lookup(11, start_key=8, inclusive=True)) == [
        (8, 9, 'h', 1),
        (9, 10, 'i', 1),
        (10, None, 'j', 1)
    ]

    assert skip_list.query(0, 11) == 10
    assert skip_list.query(1, 11) == 10
    assert skip_list.query(1.5, 3) == 2
    assert skip_list.query(2, 9) == 8
    assert skip_list.query(None, None) == 10

    skip_list[5.5] = 'X'
    # print(skip_list[5.5])
