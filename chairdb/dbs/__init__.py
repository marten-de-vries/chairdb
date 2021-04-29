from .abstract import AbstractDatabase
from .backends.memory import InMemoryBackend
from .backends.sql import SQLBackend
from .apimixins import TransactionBasedDBMixin, ContinuousChangesMixin
from .remote import HTTPDatabase
from .sync import AbstractSyncDatabase


__all__ = ('InMemoryDatabase', 'SyncInMemoryDatabase',
           'SQLDatabase',      'SyncSQLDatabase',
           'HTTPDatabase',     'SyncHTTPDatabase')


# create backend-backed dbs

class InMemoryDatabase(AbstractDatabase, TransactionBasedDBMixin,
                       ContinuousChangesMixin):
    _Backend = InMemoryBackend


class SQLDatabase(AbstractDatabase, TransactionBasedDBMixin,
                  ContinuousChangesMixin):
    _Backend = SQLBackend


# create sync wrappers

class SyncInMemoryDatabase(AbstractSyncDatabase):
    _AsyncDatabase = InMemoryDatabase


class SyncSQLDatabase(AbstractSyncDatabase):
    _AsyncDatabase = SQLDatabase


class SyncHTTPDatabase(AbstractSyncDatabase):
    _AsyncDatabase = HTTPDatabase
