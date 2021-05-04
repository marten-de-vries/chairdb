import contextlib
import uuid
import typing

from .complexkey import complex_key, parse_complex_key
from .dbs import InMemoryDatabase
from .datatypes import Document
from .errors import NotFound
from .utils import anext, verify_no_attachments


class View:
    def __init__(self, db, map, reduce=None, rereduce=None, name=None):
        self._db = db
        self._view_db = InMemoryDatabase('_view' + (name or uuid.uuid4().hex))

        self._map = map
        self._reduce = reduce
        self._rereduce = rereduce

    async def build(self):
        last_seq = await self._view_db.read_local('_local_seq')
        async for change in self._db.changes(since=last_seq):
            async with self._view_db.read_transaction() as view_rt:
                await self._process_change(view_rt, change)

    async def _process_change(self, view_rt, change):
        info = await view_rt.read_local(change.id)
        # get old key documents
        old_docs = {}
        for key in (info or {}).get('old_keys', []):
            full_key = complex_key([key, change.id])
            old_docs[full_key] = await anext(view_rt.read(full_key,
                                                          body=False))
        # build new key documents
        doc = await anext(self._db.read(change.id))
        new_docs, new_keys = [], []
        # first, we determine the new keys through mapping
        for key, value in self._map(doc):
            new_keys.append(key)
            new_docs.append(await self._build_new_doc(doc, key, value, view_rt,
                                                      old_docs))
        async with self._view_db.write_transaction() as wt:
            # write the new docs
            for doc in new_docs:
                wt.write(doc)
            # delete the non-repurposed old docs:
            for old_doc in old_docs.values():
                old_doc.is_deleted = True
                old_doc.update_rev()
                wt.write(old_doc)
            # update the delete index
            wt.write_local(change.id, {'old_keys': new_keys})
            # and finally, update the meta doc
            wt.write_local('_local_seq', change.seq)

    async def _build_new_doc(self, doc, key, value, view_rt, old_docs):
        full_key = complex_key([key, doc.id])
        # key already in the view? re-use the current doc as a base
        try:
            new_doc = old_docs.pop(full_key)
        except KeyError:
            # otherwise, find the (deleted) doc for this key
            try:
                new_doc = await anext(view_rt.read(full_key, body=False))
            except NotFound:
                # no such key yet, use it for the first time
                new_doc = Document(full_key, rev_num=0, path=(), body={})
        new_doc.body = {'value': value}
        new_doc.update_rev()
        return new_doc

    async def aggregate(self, *, start_key=None, end_key=None,
                        descending=False, group_level=0):
        query_results = self.query(start_key=start_key, end_key=end_key,
                                   descending=descending)

        current_key, current_values = None, []
        async for (key, value, *_) in query_results:
            grouped_key = None if group_level == 0 else key[:group_level]
            if grouped_key != current_key:
                if current_values:
                    yield current_key, self._rereduce(current_values)
                current_key, current_values = grouped_key, []
            current_values.append(self._reduce([key], [value]))
        yield current_key, self._rereduce(current_values)

    async def query(self, **opts):
        verify_no_attachments(opts.get('doc_opts', {}))

        async with self.query_with_attachments(**opts) as resp:
            async for result in resp:
                yield result

    @contextlib.asynccontextmanager
    async def query_with_attachments(self, *, start_key=None, end_key=None,
                                     descending=False, doc_opts=None):
        # start_key and end_key can be tuples of arity 2: (key, doc_id)
        if isinstance(start_key, str):
            start_key = [start_key]
        if start_key:
            start_key = complex_key(start_key)
        if isinstance(end_key, str):
            end_key = [end_key, {}]
        if end_key:
            end_key = complex_key(end_key)

        await self.build()

        t_call = self._db.read_transaction()
        vt_call = self._view_db.read_transaction()
        async with t_call as t, vt_call as vt:
            all = vt.all_docs(start_key=start_key, end_key=end_key,
                              descending=descending, doc_opts={'body': True,
                                                               'atts': False})
            yield self._transform(t, all, doc_opts)

    async def _transform(self, t, view_resp, doc_opts):
        async for v_doc in view_resp:
            key, id = parse_complex_key(v_doc.id)
            doc = None
            if doc_opts is not None:
                doc = await anext(t.read(id, **doc_opts))
            yield QueryResult(key, v_doc['value'], id, doc)


class QueryResult(typing.NamedTuple):
    key: typing.Any
    value: typing.Any
    id: typing.Any
    document: typing.Optional[Document] = None
