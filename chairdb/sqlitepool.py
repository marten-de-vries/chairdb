import aiosqlite
import anyio

import contextlib

MAX_PARALLEL_READS = 10


class SQLitePool:
    def __init__(self, url):
        self.url = url
        self.free = []
        self.read_semaphore = anyio.Semaphore(MAX_PARALLEL_READS)
        self.write_lock = anyio.Lock()

    @contextlib.asynccontextmanager
    async def _transaction(self):
        try:
            conn = self.free.pop()
        except IndexError:
            conn = await aiosqlite.connect(self.url)
            await conn.execute('PRAGMA journal_mode=WAL')
        await conn.execute('BEGIN')
        try:
            yield conn
        except Exception:
            await conn.execute('ROLLBACK')
            raise
        await conn.execute('COMMIT')
        self.free.append(conn)

    @contextlib.asynccontextmanager
    async def read_transaction(self):
        async with self.read_semaphore, self._transaction() as conn:
            yield conn

    @contextlib.asynccontextmanager
    async def write_transaction(self):
        async with self.write_lock, self._transaction() as conn:
            yield conn


@contextlib.asynccontextmanager
async def sqlite_pool(url):
    pool = SQLitePool(url)
    yield pool
    for conn in pool.free:
        await conn.close()
