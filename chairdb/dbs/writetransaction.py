import uuid

from .shared import chunk_id
from ..utils import aenumerate


class WriteTransaction:
    def __init__(self, tg, backend, actions):
        self._tg = tg
        self._backend = backend
        self._actions = actions

    def write(self, doc, check_conflict=False):
        chunk_info = {}
        for name, att in (doc.attachments or {}).items():
            if not att.is_stub:
                self._tg.start_soon(self._store_att, chunk_info, name, att)
        self._actions.append(('write', chunk_info, doc, check_conflict))

    async def _store_att(self, chunk_info, name, att):
        # we keep track of the total attachment size so far after each chunk to
        # make efficient retrieval of parts of the attachment possible using
        # bisection
        att_id = uuid.uuid1().hex
        current_end = 0
        chunk_ends = []
        async for i, chunk in aenumerate(att):
            async with self._backend.write_transaction() as t:
                t.write_local(chunk_id(att_id, i), chunk)
            current_end += len(chunk)
            chunk_ends.append(current_end)
        chunk_info[name] = att_id, chunk_ends

    def write_local(self, id, doc):
        self._actions.append(('write_local', id, doc))

    revs_limit = property()

    @revs_limit.setter
    def revs_limit(self, value):
        self._actions.append(('write_local', '_revs_limit', value))
