import anyio

import typing

from .revtree import RevisionTree
from ..datatypes import AttachmentStub, AttachmentMetadata, Change, Missing
from ..errors import NotFound, PreconditionFailed


# internal
class AttachmentRecord(typing.NamedTuple):
    meta: AttachmentMetadata
    data_ptr: typing.Any


def decode_att_store(store):
    return {
        name: AttachmentRecord(AttachmentMetadata(*meta), ptr)
        for name, (meta, ptr) in store.items()
    }


async def as_future_result(value):
    await anyio.sleep(0)
    return value


async def get_revs_limit(t):
    return await t.read_local('_revs_limit') or 1000  # default


def chunk_id(att_id, i):
    return f'_chunk_{att_id}_{i:020}'


async def get_rev_tree(t, id):
    try:
        return await t.read(id)
    except NotFound:
        return RevisionTree()


def update_atts(store, new_attachments, chunk_info):
    new_store = {}
    for name, new_att in new_attachments.items():
        if new_att.is_stub:
            new_store[name] = reuse_record(store, name, new_att)
        else:
            att_id, chunk_ends = chunk_info[name]
            # TODO: actually store & use the bisection info (chunk_ends)
            new_store[name] = AttachmentRecord(new_att.meta, att_id)
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


def all_docs_branch(rev_tree):
    branch = rev_tree.winner()
    if branch.leaf_doc_ptr:  # not deleted
        yield branch


def build_change(id, seq, rev_tree):
    deleted = rev_tree.winner().leaf_doc_ptr is None
    leaf_revs = [branch.leaf_rev_tuple for branch in rev_tree.branches()]
    return Change(id, seq, deleted, leaf_revs)


def revs_diff(id, revs, rev_tree):
    missing, possible_ancestors = set(), set()
    for rev in revs:
        is_missing, new_possible_ancestors = rev_tree.diff(*rev)
        if is_missing:
            missing.add(rev)
            possible_ancestors.update(new_possible_ancestors)
    return Missing(id, missing, possible_ancestors)


def read_docs(id, revs, rev_tree):
    # ... walk the revision tree
    if revs is None:
        yield rev_tree.winner()
    elif revs == 'all':
        # all leafs
        yield from rev_tree.branches()
    else:
        # some leafs
        for rev in revs:
            yield from rev_tree.find(*rev)


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


def record_change_since(record_rev_num, revs, branch):
    return not (revs is None or any(
        record_rev_num <= rev_num and branch.contains(rev_num, rev_hash)
        for rev_num, rev_hash in revs
    ))
