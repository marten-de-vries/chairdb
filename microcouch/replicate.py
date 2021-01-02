import aioitertools

import hashlib
import uuid

from .errors import NotFound
from .utils import async_iter

REPLICATION_ID_VERSION = 1


async def replicate(source, target, create_target=False, continuous=False):
    # Verify Peers - you should expect to handle 401, 403 and 404
    # HTTPStatusError-s.

    source_update_seq = await source.update_seq
    target_update_seq = await get_target_seq(target, create_target)

    session_id = uuid.uuid4().hex
    replication_id = await gen_repl_id(source, target, create_target,
                                       continuous)
    log_request = async_iter([(f'_local/{replication_id}', 'winner')])
    source_log = await aioitertools.next(source.read(log_request))
    print(type(source_log))
    target_log = await aioitertools.next(target.read(log_request))
    print(type(target_log))


async def get_target_seq(target, create_target):
    try:
        return await target.update_seq
    except NotFound:
        if create_target:
            await target.create()
            # second chance
            return await target.update_seq
        raise


async def gen_repl_id(source, target, create_target, continuous):
    repl_id_values = ''.join([
        await source.id,
        await target.id,
        str(create_target),
        str(continuous),
    ]).encode('UTF-8')
    return hashlib.md5(repl_id_values).hexdigest()


# Testing code below:

async def main():
    from . import InMemoryDatabase
    from . import HTTPDatabase
    source = HTTPDatabase('http://localhost:5984/activiteitenweger',
                          credentials=('marten', 'test'))
    target = InMemoryDatabase(id='test')  # guarantee stable replication id
    async with source:
        await replicate(source, target, create_target=True)


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
