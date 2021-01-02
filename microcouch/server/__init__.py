from starlette.applications import Starlette
from starlette.routing import Mount, Route
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware

import sortedcontainers
import uuid

from .. import InMemoryDatabase
from .db import build_db_app
from .utils import JSONResp


__version__ = "0.1"

DB_NOT_FOUND = {
    "error": "not_found",
    "reason": "Database does not exist.",
}

DB_EXISTS = {
    "error": "file_exists",
    "reason": "The database could not be created, the file already exists.",
}

SUCCESS = {
    "ok": True,
}


def root(request):
    return JSONResp({
        "microcouch": "Welcome!",
        "version": __version__,
        "uuid": request.app.state.server_id.hex,
        "features": [],
        "vendor": {
            "name": "Marten de Vries"
        }
    })


def put_db(request):
    dbname = request.path_params['db']
    if dbname in request.app.state.dbs:
        return JSONResp(DB_EXISTS, 412)
    request.app.state.dbs[dbname] = InMemoryDatabase()
    return JSONResp(SUCCESS, 201)


def all_dbs(request):
    return JSONResp(list(request.app.state.dbs.keys()))


class DBLoaderMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        try:
            request.state.db = app.state.dbs[request.path_params['db']]
        except KeyError:
            return JSONResp(DB_NOT_FOUND, 404)
        return await call_next(request)


db_app = build_db_app(middleware=[Middleware(DBLoaderMiddleware)])

app = Starlette(routes=[
    Route('/', root),
    Route('/_all_dbs', all_dbs),
    Route('/{db}/', put_db, methods=['PUT']),
    Mount('/{db}', db_app),
])

app.state.dbs = sortedcontainers.SortedDict()
app.state.server_id = uuid.uuid4()
