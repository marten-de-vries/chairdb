from chairdb import complex_key, parse_complex_key
import math


def test_sort_numbers():
    nums = [float('-inf'), -3.8e10, -2, -1, -.1, 0, 1, 2, math.pi, 99e600,
            float('inf')]
    encoded = [complex_key(x) for x in nums]
    assert encoded == sorted(encoded)
    assert [parse_complex_key(x) for x in encoded] == nums


def test_array_and_object_key():
    # based on the pouchdb-collate example
    obj = {'key1': ['value'], 'key2': None}
    input = [67, True, 'McDuck', '', 'Scrooge', {}, obj]
    encoded = complex_key(input)
    assert input == parse_complex_key(encoded)
