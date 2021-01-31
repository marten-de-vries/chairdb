import typing
from .datatypes import AttachmentMetadata, AttachmentStub, PreconditionFailed


class AttachmentRecord(typing.NamedTuple):
    meta: AttachmentMetadata
    data_ptr: typing.Any


class AttachmentStore(dict):
    """Maps attachment names to AttachmentDescription objects"""

    def read(self, branch, names, since_revs):
        """since_revs=[] means 'return all attachments'. since_revs=None means
        'return no attachments' (except for those named in 'names')

        """
        names = set(names or [])
        result = {}
        todo = []
        for name, att_record in self.items():
            rec_rev_num = att_record.meta.rev_pos
            changed = self.record_change_since(rec_rev_num, since_revs, branch)
            if name in names or changed:
                todo.append((name, att_record))
            else:
                result[name] = AttachmentStub(att_record.meta)
        return result, todo

    def record_change_since(self, record_rev_num, revs, branch):
        return not (revs is None or any(
            record_rev_num <= rev_num and branch.contains(rev_num, rev_hash)
            for rev_num, rev_hash in revs
        ))

    def merge(self, new_attachments):
        done, old, new = {}, [], []

        # overwrite stubs
        for name, new_att in new_attachments.items():
            if new_att.is_stub:
                done[name] = self._reuse_record(name, new_att)
            else:
                new.append((name, new_att))

        # we delay updates until now so we don't change anything when an
        # exception is raised above.
        for name, att in list(self.items()):
            if name not in new_attachments:
                old.append(att.data_ptr)
                del self[name]
        self.update(done)

        # let the caller add the actually new items to the database, as that
        # requires database-specific logic
        return old, new

    def _reuse_record(self, name, new_att):
        try:
            old_att = self[name]
        except KeyError:
            raise PreconditionFailed('stub without attachment')
        if new_att.meta.rev_pos != old_att.meta.rev_pos:
            raise PreconditionFailed('stub with wrong rev_pos')
        # re-use the existing one, but allow changing e.g. the
        # content-type. CouchDB does too...
        return AttachmentRecord(new_att.meta, old_att.data_ptr)

    def add(self, name, meta, data_ptr):
        self[name] = AttachmentRecord(meta, data_ptr)
