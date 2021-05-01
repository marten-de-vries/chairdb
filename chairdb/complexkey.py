"""JSON-like values as ids or keys instead of 'just' strings.

This module implements an encoder and decoder for a binary serialization
format of JSON-like values. The format is build such that if you sort the
encoded values, they will be sorted in the following order:

1. null values
2. booleans
3. numbers
4. string
5. arrays
6. objects

Of course, item in arrays/objects are in turn sorted using the same criteria.
Booleans, numbers and strings are sorted by their values. This is similar to
the CouchDB collation ordering, but not quite the same as our strings are
represented as plain UTF-8 instead of following ICU string ordering. PouchDB
has the same issue, but it doesn't really show up much in practise.

Inspired by (but not compatible with) pouchdb-collate

"""
import collections.abc
import numbers
import struct

# specifies the order
(END, NONE, FALSE, TRUE, NEGATIVE_NUMBER, ZERO, POSITIVE_NUMBER, EMPTY_STRING,
 STRING, EMPTY_ARRAY, ARRAY, EMPTY_OBJECT, OBJECT) = range(13)

END_MARKER, ARRAY_MARKER, OBJECT_MARKER = object(), object(), object()


def complex_key(start_key):
    stack = [start_key]
    result = bytearray()
    while stack:
        key = stack.pop()
        tag = determine_tag(key)
        result += bytes([tag])
        if tag == ARRAY:
            serialize_array(key, stack)
        elif tag == OBJECT:
            serialize_object(key, stack)
        else:
            serialize_value(tag, key, result)
    return bytes(result)


def determine_tag(key):
    if key is END_MARKER:
        return END
    if key is None:
        return NONE
    if key is False:
        return FALSE
    if key is True:
        return TRUE
    return determine_complex_tag(key)


def determine_complex_tag(key):
    if isinstance(key, numbers.Number):
        return determine_number_tag(key)

    options = [(str, EMPTY_STRING, STRING),
               (collections.abc.Sequence, EMPTY_ARRAY, ARRAY),
               (collections.abc.Mapping, EMPTY_OBJECT, OBJECT)]
    for cls, empty_tag, tag in options:
        if isinstance(key, cls):
            if not key:
                return empty_tag
            return tag
    raise ValueError(f'unknown key type: {type(key)}')


def determine_number_tag(key):
    if key < 0:
        return NEGATIVE_NUMBER
    elif key == 0:
        return ZERO
    else:
        return POSITIVE_NUMBER


def serialize_value(tag, key, result):
    if tag == NEGATIVE_NUMBER:
        double_bytes = serialize_num(key)
        # Flipping means bigger negative numbers come first. See for more info:
        # https://stackoverflow.com/a/43305015
        result += flip_bytes(double_bytes)
    elif tag == POSITIVE_NUMBER:
        result += serialize_num(key)
    elif tag == STRING:
        result += key.encode('UTF-8')
        result += b'\0'


def serialize_num(num):
    return struct.pack('>d', num)


def flip_bytes(the_bytes):
    return bytes(b ^ 0xff for b in the_bytes)


def serialize_array(array, stack):
    stack.append(END_MARKER)
    stack.extend(reversed(array))


def serialize_object(object, stack):
    stack.append(END_MARKER)
    for key, value in reversed(object.items()):
        stack.append(value)
        stack.append(key)


def parse_complex_key(value):
    stack = [[]]
    i = 0
    while i < len(value):
        tag = value[i]
        i += 1
        try:
            # constants
            stack[-1].append({
                NONE: None,
                FALSE: False,
                TRUE: True,
                ZERO: 0,
                EMPTY_STRING: '',
                EMPTY_ARRAY: [],
                EMPTY_OBJECT: {},
            }[tag])
        except KeyError:
            i = parse_complex_tag(value, i, tag, stack)
    assert len(stack) == 1 and len(stack[0]) == 1
    return stack[0][0]


def parse_complex_tag(value, i, tag, stack):
    # more involved parsing
    if tag == END:
        data = stack.pop()
        if data[0] is ARRAY_MARKER:
            stack[-1].append(data[1:])
        else:
            stack[-1].append(deserialize_object(data))
    elif tag == ARRAY:
        stack.append([ARRAY_MARKER])
    elif tag == OBJECT:
        stack.append([OBJECT_MARKER])
    else:
        i = parse_value_tag(value, i, tag, stack)
    return i


def parse_value_tag(value, i, tag, stack):
    if tag == NEGATIVE_NUMBER:
        stack[-1].append(deserialize_neg_number(value[i:i + 8]))
        i += 8
    elif tag == POSITIVE_NUMBER:
        stack[-1].append(deserialize_number(value[i:i + 8]))
        i += 8
    elif tag == STRING:
        string, delta_i = deserialize_str(value, i)
        stack[-1].append(string)
        i += delta_i
    else:
        raise ValueError(f'Unknown tag: {tag}')
    return i


def deserialize_neg_number(value):
    return deserialize_number(flip_bytes(value))


def deserialize_number(value):
    return struct.unpack('>d', value)[0]


def deserialize_str(value, start):
    end = value.index(b'\0', start)
    return value[start:end].decode('UTF-8'), end - start + 1


def deserialize_object(data):
    result = {}
    for pos, item in enumerate(data):
        if pos == 0:
            assert item is OBJECT_MARKER
        elif pos % 2 == 1:
            key = item
        else:
            result[key] = item
    return result
