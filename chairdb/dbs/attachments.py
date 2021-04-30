import bisect
import typing

from ..errors import PreconditionFailed
from ..datatypes import AttachmentMetadata, AttachmentStub


class AttachmentRecord(typing.NamedTuple):
    meta: AttachmentMetadata
    data_ptr: typing.Any


def read_atts(att_store_raw, branch, selector):
    att_store = decode_att_store(att_store_raw)
    names, since_revs = selector
    result = {}
    todo = []
    for name, att_record in att_store.items():
        rec_rev_num = att_record.meta.rev_pos
        changed = record_change_since(rec_rev_num, since_revs, branch)
        if name in names or changed:
            todo.append((name, att_record))
        else:
            result[name] = AttachmentStub(att_record.meta)
    return result, todo


def decode_att_store(store):
    return {
        name: AttachmentRecord(AttachmentMetadata(*meta), ptr)
        for name, (meta, ptr) in store.items()
    }


def record_change_since(record_rev_num, revs, branch):
    return not (revs is None or any(
        record_rev_num <= rev_num and branch.contains(rev_num, rev_hash)
        for rev_num, rev_hash in revs
    ))


def update_atts(store_raw, new_attachments, chunk_info):
    store = decode_att_store(store_raw)
    new_store = {}
    for name, new_att in new_attachments.items():
        if new_att.is_stub:
            new_store[name] = reuse_record(store, name, new_att)
        else:
            new_store[name] = AttachmentRecord(new_att.meta, chunk_info[name])
    return new_store


def reuse_record(store, name, new_att):
    try:
        old_att = store[name]
    except KeyError:
        raise PreconditionFailed('stub without attachment')
    if new_att.meta.rev_pos != old_att.meta.rev_pos:
        raise PreconditionFailed('stub with wrong rev_pos')
    # re-use the existing one, but allow changing e.g. the
    # content-type. CouchDB does too...
    return AttachmentRecord(new_att.meta, old_att.data_ptr)


def chunk_id(att_id, i):
    return f'_chunk_{att_id}_{i:020}'


def slice_att(data_ptr, slice):
    assert slice.step is None
    att_id, chunk_ends = data_ptr

    start_chunk_i, start_offset = find_start(slice.start, chunk_ends)
    end_chunk_i, end_offset = find_end(slice.stop, chunk_ends)

    start_key = chunk_id(att_id, start_chunk_i)
    end_key = chunk_id(att_id, end_chunk_i)

    last_i = end_chunk_i - start_chunk_i
    return start_key, end_key, start_offset, end_offset, last_i


def find_start(start, chunk_ends):
    if start is None:
        return 0, None
    start_chunk_i = bisect.bisect_right(chunk_ends, start)
    return start_chunk_i, start - chunk_start(chunk_ends, start_chunk_i)


def find_end(stop, chunk_ends):
    if stop is None:
        return len(chunk_ends) - 1, None
    end_chunk_i = bisect.bisect_left(chunk_ends, stop)
    return end_chunk_i, stop - chunk_start(chunk_ends, end_chunk_i)


def chunk_start(chunk_ends, chunk_i):
    if chunk_i == 0:
        return 0
    return chunk_ends[chunk_i - 1]
