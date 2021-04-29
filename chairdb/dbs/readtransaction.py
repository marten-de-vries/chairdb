import typing

from ..datatypes import AttachmentMetadata, Document, AttachmentSelector
from .shared import (get_revs_limit, chunk_id, get_rev_tree, all_docs_branch,
                     build_change, read_docs, read_atts, revs_diff)


class ReadTransaction:
    def __init__(self, t):
        self._t = t

    @property
    def update_seq(self):
        return self._t.update_seq

    @property
    def revs_limit(self):
        return get_revs_limit(self._t)

    async def all_docs(self, *, start_key=None, end_key=None, descending=False,
                       doc_opts=dict(body=False, atts=None)):
        rows = self._t.all_docs(start_key, end_key, descending)
        async for id, rev_tree in rows:
            for branch in all_docs_branch(rev_tree):
                yield await self._read_doc(id, branch, **doc_opts)

    def all_local_docs(self, *, start_key=None, end_key=None,
                       descending=False):
        return self._t.all_local_docs(start_key, end_key, descending)

    async def changes(self, since=None):
        async for seq, id, rev_tree in self._t.changes(since):
            yield build_change(id, seq, rev_tree)

    async def read(self, id, *, revs=None, body=True,
                   atts=AttachmentSelector()):
        rev_tree = await self._t.read(id)
        for branch in read_docs(id, revs, rev_tree):
            yield await self._read_doc(id, branch, body, atts)

    async def _read_doc(self, id, branch, body, atts):
        doc_ptr = branch.leaf_doc_ptr
        deleted = not doc_ptr

        doc_body, doc_atts = None, None
        if not deleted:
            if body:
                doc_body = await self._t.read_local(f'_body_{doc_ptr}')
            if atts:
                att_store = await self._t.read_local(f'_att_store_{doc_ptr}')
                doc_atts, todo = read_atts(att_store, branch, atts)
                for name, info in todo:
                    doc_atts[name] = Attachment(self, info.meta, info.data_ptr)

        return Document(id, branch.leaf_rev_num, branch.path, doc_body,
                        doc_atts, deleted)

    def read_local(self, id):
        return self._t.read_local(id)

    async def single_revs_diff(self, id, revs):
        return revs_diff(id, revs, await get_rev_tree(self._t, id))


class Attachment(typing.NamedTuple):
    t: ReadTransaction
    meta: AttachmentMetadata
    data_ptr: int
    is_stub: bool = False

    async def __aiter__(self):
        # 64-bit 'size'
        rows = self.t.all_local_docs(start_key=chunk_id(self.data_ptr, 0),
                                     end_key=chunk_id(self.data_ptr, 2**64-1))
        async for id, doc in rows:
            yield doc
