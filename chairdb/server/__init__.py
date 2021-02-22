"""A CouchDB-compatible HTTP server backed by in-memory databases."""

from starlette.applications import Starlette
from starlette.routing import Mount, Route
from starlette.middleware import Middleware
from starlette.staticfiles import StaticFiles

import sortedcontainers
import uuid
import os

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

SESSION = {
    "ok": True,
    "userCtx": {"name": None, "roles": ["_admin"]},
}


async def root(request):
    return JSONResp({
        "chairdb": "Welcome!",
        "version": __version__,
        "uuid": request.app.state.server_id.hex,
        "features": [],
        "vendor": {
            "name": "Marten de Vries"
        }
    })


async def put_db(request):
    dbname = request.path_params['db']
    if dbname in request.app.state.dbs:
        return JSONResp(DB_EXISTS, 412)
    request.app.state.dbs[dbname] = InMemoryDatabase()
    return JSONResp(SUCCESS, 201)


async def delete_db(request):
    dbname = request.path_params['db']
    request.app.state.dbs.pop(dbname, None)
    return JSONResp(SUCCESS, 200)


async def all_dbs(request):
    return JSONResp(list(request.app.state.dbs.keys()))


async def session(request):
    return JSONResp(SESSION)


class DBLoaderMiddleware:
    """Automatically load the appropriate in-memory database into db_app's
    request.state.db, or return an error if there is no such database.

    """
    def __init__(self, app):
        self.db_app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            db_name = scope['path_params']['db']
            request_state = scope.setdefault('state', {})
            try:
                request_state['db'] = app.state.dbs[db_name]
                request_state['db_name'] = db_name
            except KeyError:
                response = JSONResp(DB_NOT_FOUND, 404)
                await response(scope, receive, send)
                return
        await self.db_app(scope, receive, send)


db_app = build_db_app(middleware=[Middleware(DBLoaderMiddleware)])

fauxton_path = os.path.join(os.path.dirname(__file__), 'fauxton')
app = Starlette(routes=[
    Route('/', root),
    Route('/_all_dbs', all_dbs),
    Route('/_session', session),
    Mount('/_utils', StaticFiles(directory=fauxton_path, html=True)),
    Route('/{db}/', put_db, methods=['PUT']),
    Route('/{db}/', delete_db, methods=['DELETE']),
    Mount('/{db}', db_app),
])

# used to keep track of all the databases
app.state.dbs = sortedcontainers.SortedDict()
# for replication:
app.state.server_id = uuid.uuid4()
