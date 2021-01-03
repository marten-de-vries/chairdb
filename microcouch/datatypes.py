import typing


class MicroCouchError(Exception):
    """Base class for all custom errors."""


class Forbidden(MicroCouchError):
    """You need to log in."""


class Unauthorized(MicroCouchError):
    """You are logged in, but not allowed to do this."""


class NotFound(MicroCouchError):
    """Something (a document or database, probably) doesn't exist."""


class Change(typing.NamedTuple):
    """A representation of a row in the _changes feed"""

    id: str
    seq: int
    deleted: bool
    leaf_revs: list
