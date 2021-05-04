import sys
sys.path.insert(0, '..')

from starlette.applications import Starlette
from starlette.responses import (JSONResponse, RedirectResponse,
                                 PlainTextResponse)
from starlette.routing import Route, Mount

import nanoid
from chairdb import (sqlite_pool, View, Document, SQLDatabase, build_db_app,
                     anext)

import heapq
import uuid
from urllib.parse import urlparse


async def new_url(request):
    url = (await request.json())['url']
    assert urlparse(url).scheme == 'https'  # be overly strict

    id = nanoid.generate()
    # auto generate rev + path (DREAMCODE!)
    doc = Document(id=id, rev_num=1, path=(uuid.uuid1().hex,),
                   body={'url': url})
    await request.app.state.db.write(doc)
    return JSONResponse({'shortened_url': request.url_for('redirect', id=id),
                         'preview_url': request.url_for('preview', id=id)})


async def redirect(request):
    return RedirectResponse(await get_url(request))


async def preview(request):
    return PlainTextResponse(await get_url(request))


async def get_url(request):
    read = request.app.state.db.read(request.path_params['id'])
    return (await anext(read))['url']


async def popularity(request):
    # domain popularity: give the ten most popular domains
    heap = []
    aggregated = request.app.state.by_domain.aggregate(group_level=None)
    async for domain, count in aggregated:
        if len(heap) < 10:
            heapq.heappush(heap, (count, domain))
        else:
            heapq.heappushpop(heap, (count, domain))

    popcount_top = [heapq.heappop(heap) for i in range(len(heap))]
    popcount_top.reverse()

    text = '\n'.join(f'{url} {count}'for count, url in popcount_top)
    return PlainTextResponse(text)


async def lifespan(app):
    async with sqlite_pool('urls.sqlite3') as main_pool:
        app.state.db = SQLDatabase(main_pool)
        await app.state.db.create()
        app.state.by_domain = View(app.state.db, map, reduce, rereduce)
        db_app.state.db = app.state.db
        yield


db_app = build_db_app()


app = Starlette(routes=[
    Route('/', new_url, methods=['POST']),
    Mount('/db', db_app),
    Route('/popularity', popularity),
    Route('/preview/{id}', preview, name='preview'),
    Route('/{id}', redirect, name='redirect'),
], lifespan=lifespan)


def map(doc):
    yield urlparse(doc['url']).netloc, None


def reduce(key, value):
    return 1


def rereduce(values):
    return sum(values)


# DREAMCODE:
# db_app.state.db = app.state.db = SQLiteDatabase('db.sqlite3', 'urls')
# app.state.by_domain = db.index(map, reduce, rereduce)
