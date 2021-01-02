class MicroCouchError(Exception):
    pass


class Forbidden(MicroCouchError):
    pass


class Unauthorized(MicroCouchError):
    pass


class NotFound(MicroCouchError):
    pass
