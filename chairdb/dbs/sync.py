import anyio

from ..utils import InMemoryAttachment, to_list

import contextlib


class AbstractSyncDatabase:
    """Assumes (apart from read/write transaction) existance of:
    - _changes

    """
    def __init__(self, *args, **kwargs):
        self._db = self._AsyncDatabase(*args, **kwargs)

    def revs_diff_sync(self, id, revs):
        return anyio.run(self._db.single_revs_diff, id, revs)

    def all_docs_sync(self, **opts):
        return iter(anyio.run(to_list, self._db.all_docs(**opts)))

    def changes_sync(self, since=None, continuous=False):
        return iter(anyio.run(to_list, self._db.changes(since, continuous)))

    def write_sync(self, doc):
        anyio.run(self._db.write, doc)

    def write_local_sync(self, id, doc):
        anyio.run(self._db.write_local, id, doc)

    def read_sync(self, *args, **kwargs):
        return iter(anyio.run(to_list, self._db.read(*args, **kwargs)))

    @contextlib.contextmanager
    def read_with_attachments_sync(self, *args, **kwargs):
        read = self._db.read_with_attachments(*args, **kwargs)
        with syncify_ctx_manager(read) as aiter:
            yield syncify_docs(aiter)

    def read_local_sync(self, id):
        return anyio.run(self._db.read_local, id)

    @property
    def revs_limit_sync(self):
        return anyio.run(lambda: self._db.revs_limit)

    @revs_limit_sync.setter
    def revs_limit_sync(self, limit):
        return anyio.run(self._db.set_revs_limit, limit)


@contextlib.contextmanager
def syncify_ctx_manager(ctx):
    # assumes no error handler in the async context manager
    result = anyio.run(ctx.__aenter__)
    try:
        yield result
    finally:
        anyio.run(ctx.__aexit__, None, None, None)


def syncify_docs(docs):
    for doc in anyio.run(to_list, docs):
        for name in (doc.attachments or {}):
            async_att = doc.attachments[name]
            if not async_att.is_stub:
                data = b''.join(anyio.run(to_list, async_att))
                att = InMemoryAttachment(async_att.meta, data, False)
                doc.attachments[name] = att
        yield doc
