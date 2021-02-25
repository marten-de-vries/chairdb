import base64
import hashlib
import mimetypes
import typing


class ChairDBError(Exception):
    """Base class for all custom errors."""


class Forbidden(ChairDBError):
    """You need to log in."""


class Unauthorized(ChairDBError):
    """You are logged in, but not allowed to do this."""


class NotFound(ChairDBError):
    """Something (a document or database, probably) doesn't exist."""


class PreconditionFailed(ChairDBError):
    """Wrong assumption"""


class Change(typing.NamedTuple):
    """A representation of a row in the _changes feed"""

    id: str
    seq: int
    deleted: bool
    leaf_revs: typing.List[str]


class Missing(typing.NamedTuple):
    id: str
    missing_revs: typing.List[str]
    possible_ancestors: typing.List[str]


class DataType:
    __slots__ = ()

    def __repr__(self):
        args = ', '.join(f'{key}={repr(getattr(self, key))}'
                         for key in self._keys())
        return f'{type(self).__name__}({args})'

    def _keys(self):
        for cls in reversed(type(self).__mro__):
            yield from getattr(cls, '__slots__', ())

    def __eq__(self, other):
        return (
            type(self) == type(other) and
            all(getattr(self, k) == getattr(other, k) for k in self._keys())
        )


class AbstractDocument(DataType):
    __slots__ = ('id', 'body', '_is_deleted')

    def __init__(self, id, body, is_deleted=False):
        if is_deleted:
            assert body is None

        self.id = id
        self.body = body
        self.is_deleted = is_deleted

    def _get_deleted(self):
        return self._is_deleted

    def _set_deleted(self, deleted):
        self._is_deleted = deleted
        if deleted:
            self.body = None
        elif not self.body:
            self.body = {}

    is_deleted = property(fget=_get_deleted, fset=_set_deleted)


class Document(AbstractDocument):
    __slots__ = ('rev_num', 'path', 'attachments')

    def __init__(self, id, rev_num, path, body=None, attachments=None,
                 is_deleted=False):
        if is_deleted:
            assert attachments is None

        self.rev_num = rev_num
        self.path = path
        self.attachments = attachments

        super().__init__(id, body, is_deleted)

    # proxy
    def __getitem__(self, key):
        return self.body[key]

    def add_attachment(self, name, iterator, content_type=None):
        if not content_type:
            content_type = mimetypes.guess_type(name)[0]
        attachment = NewAttachment(self.rev_num, content_type, iterator)
        self.attachments[name] = attachment

    def _set_deleted(self, deleted):
        super()._set_deleted(deleted)
        if deleted:
            self.attachments = None
        elif not self.attachments:
            self.attachments = {}

    is_deleted = property(fget=AbstractDocument._get_deleted,
                          fset=_set_deleted)


class NewAttachment:
    """NOTE: don't mix async/sync APIs."""
    is_stub = False

    def __init__(self, rev_pos, content_type, iterator):
        self._args = (rev_pos, content_type)
        self._hash = hashlib.md5()
        self._length = 0
        self._iterator = iterator

    async def __aiter__(self):  # for async API users
        async for chunk in self._iterator:
            yield self.process_chunk(chunk)

    def __iter__(self):  # for sync API users
        for chunk in self._iterator:
            yield self.process_chunk(chunk)

    def process_chunk(self, chunk):
        self._length += len(chunk)
        self._hash.update(chunk)
        return chunk

    @property
    def meta(self):
        digest = f'md5-{base64.b64encode(self._hash.digest()).decode("ascii")}'
        return AttachmentMetadata(*self._args, self._length, digest)


class AttachmentMetadata(typing.NamedTuple):
    rev_pos: int
    content_type: str
    length: int
    digest: str


class AttachmentStub(typing.NamedTuple):
    meta: AttachmentMetadata

    @property
    def is_stub(self):
        return True
