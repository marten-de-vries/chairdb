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


class Document(typing.NamedTuple):
    id: str
    rev_num: int = 0
    path: typing.Optional[typing.List[str]] = None
    body: typing.Optional[dict] = None

    @property
    def deleted(self):
        return self.body is None

    @property
    def is_local(self):
        return self.path is None

    # proxy
    def __getitem__(self, key):
        return self.body[key]
