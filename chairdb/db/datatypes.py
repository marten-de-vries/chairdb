import inspect
import typing


class ChairDBError(Exception):
    """Base class for all custom errors."""


class Forbidden(ChairDBError):
    """You need to log in."""


class Unauthorized(ChairDBError):
    """You are logged in, but not allowed to do this."""


class NotFound(ChairDBError):
    """Something (a document or database, probably) doesn't exist."""


class Change(typing.NamedTuple):
    """A representation of a row in the _changes feed"""

    id: str
    seq: int
    deleted: bool
    leaf_revs: typing.List[str]


class Missing(typing.NamedTuple):
    id: str
    missing_revs: typing.List[str]


class AbstractDocument:
    __slots__ = ('id', 'body')

    def __init__(self, id, body):
        self.id = id
        self.body = body

    @property
    def deleted(self):
        # TODO: consider how doc.deleted relates to NotFound. Also for local
        # docs
        return self.body is None

    # proxy
    def __getitem__(self, key):
        return self.body[key]

    def __repr__(self):
        args = ', '.join(f'{key}={repr(getattr(self, key))}'
                         for key in self._keys())
        return f'{type(self).__name__}({args})'

    def _keys(self):
        return inspect.signature(self.__init__).parameters.keys()

    def __eq__(self, other):
        return (
            type(self) == type(other) and
            all(getattr(self, k) == getattr(other, k) for k in self._keys())
        )


class Document(AbstractDocument):
    __slots__ = ('rev_num', 'path')

    def __init__(self, id, rev_num, path, body):
        super().__init__(id, body)

        self.rev_num = rev_num
        self.path = path
