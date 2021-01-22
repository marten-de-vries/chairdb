import ijson

import contextlib
import json


# JSON helpers
def as_json(item):
    return json.dumps(item, separators=(",", ":"), cls=SetEncoder)


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        assert isinstance(obj, set)
        return list(obj)


async def parse_json_stream(stream, type, prefix):
    results = ijson.sendable_list()
    coro = getattr(ijson, type + '_coro')(results, prefix)
    async for chunk in stream:
        with contextlib.suppress(StopIteration):
            coro.send(chunk)
        for result in results:
            yield result
        results.clear()


# async helpers
async def aenumerate(iterable):
    counter = 0
    async for item in iterable:
        yield counter, item
        counter += 1


async def async_iter(iterable):
    for item in iterable:
        yield item


async def to_list(asynciterable):
    return [x async for x in asynciterable]


async def peek(aiterable, n=2):
    first_n = await to_list(take_n(aiterable, n))
    return first_n, combine(first_n, aiterable)


async def take_n(aiterable, n):
    async for i, item in aenumerate(aiterable):
        yield item
        if i + 1 == n:
            break


async def combine(iterable, aiterable):
    for item in iterable:
        yield item
    async for item in aiterable:
        yield item
