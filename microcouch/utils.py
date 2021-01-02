import json
import typing


def rev(rev_num, rev_info):
    return f'{rev_num}-{rev_info.rev_hash}'


def parse_rev(rev):
    num, hash = rev.split('-')
    return int(num), hash


async def async_iter(iterable):
    for item in iterable:
        yield item


async def to_list(asynciterable):
    return [x async for x in asynciterable]


class Change(typing.NamedTuple):
    id: str
    seq: int
    deleted: bool
    leaf_revs: list


def as_json(item):
    return json.dumps(item, separators=(",", ":"), cls=SetEncoder)


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)
