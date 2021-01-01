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
