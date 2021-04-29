import base64
import hashlib
import json
import mimetypes
import typing


class Change(typing.NamedTuple):
    """A representation of a row in the _changes feed"""

    id: str
    seq: int
    deleted: bool
    leaf_revs: typing.List[str]


RevisionTuple = typing.Tuple[int, str]


class Missing(typing.NamedTuple):
    id: str
    missing_revs: typing.List[RevisionTuple]
    possible_ancestors: typing.List[RevisionTuple]


class AttachmentSelector(typing.NamedTuple):
    """since_revs=() means 'return all attachments'. since_revs=None means
    'return no attachments' (except for those named in 'names')

    """
    names: typing.Tuple[str, ...] = ()
    since_revs: typing.Optional[typing.Tuple[RevisionTuple, ...]] = None

    @classmethod
    def all(cls):
        return cls(since_revs=())


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
    __slots__ = ('id', 'body', 'is_deleted')

    def __init__(self, id, body=None, is_deleted=False):
        if is_deleted:
            assert body is None

        self.id = id
        self.body = body
        self.is_deleted = is_deleted


placeholder = object()


class Document(AbstractDocument):
    __slots__ = ('rev_num', 'path', 'attachments')

    def __init__(self, id, rev_num, path, body=None, attachments=placeholder,
                 is_deleted=False):
        # Using lists messes up the internal state of the rev tree. After the
        # first time I encountered that issue, I switched the property from a
        # list to a tuple. The second time, I added this assertion. Let's hope
        # there won't be a third time. :D
        assert isinstance(path, tuple)
        if attachments is placeholder:
            attachments = None if is_deleted else {}
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

    def update_rev(self):
        hash = hashlib.md5()
        hash.update(json.dumps(self.id).encode('UTF-8'))
        hash.update(self._encode_int(self.rev_num))
        for prev_hash in self.path:
            hash.update(prev_hash.encode('UTF-8'))
        hash.update(str(self.is_deleted).encode('UTF-8'))
        hash.update(json.dumps(self.body).encode('UTF-8'))
        for name, att in self.attachments.items():
            hash.update(name.encode('UTF-8'))
            hash.update(self._encode_int(att.meta.rev_pos))
            hash.update(att.meta.content_type.encode('UTF-8'))
            hash.update(self._encode_int(att.meta.length))
            hash.update(att.meta.digest.encode('UTF-8'))

        self.rev_num += 1
        self.path = (hash.hexdigest(),) + self.path

    def _encode_int(self, num):
        return num.to_bytes(8, 'big')


class NewAttachment:
    is_stub = False

    def __init__(self, rev_pos, content_type, iterator):
        self._args = (rev_pos, content_type)
        self._hash = hashlib.md5()
        self._length = 0
        self._iterator = iterator

    async def __aiter__(self):
        try:
            async for chunk in self._iterator:
                yield self.process_chunk(chunk)
        except TypeError:  # for the convenience of sync users
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
