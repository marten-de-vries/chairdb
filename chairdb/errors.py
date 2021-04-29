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


class Conflict(ChairDBError):
    """Another revision conflicts with the one you're trying to insert"""
