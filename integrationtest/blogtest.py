import sys
sys.path.insert(0, '..')

from chairdb import InMemoryDatabase, replicate, Document

async def main():
    server_db = InMemoryDatabase()
    jane_db = InMemoryDatabase()
    bob_db = InMemoryDatabase()

    server_db.write_sync(Document('roadside', 1, ('1a9c',), {'trees_count': 40}))
    await replicate(source=server_db, target=jane_db)
    await replicate(source=server_db, target=bob_db)

    bob_db.write_sync(Document('roadside', 2, ('e3b0', '1a9c'), {'trees_count': 41}))
    jane_db.write_sync(Document('roadside', 2, ('6e05', '1a9c'), {'trees_count': 41}))
    await replicate(source=jane_db, target=server_db)
    await replicate(source=bob_db, target=server_db)

    for doc in server_db.read_sync('roadside', revs='all'):
        print(doc)

    server_db.write_sync(Document('roadside', 3, ('b617', '6e05', '1a9c'),
                                  body=None))
    server_db.write_sync(Document('roadside', 3, ('5bd6', 'e3b0', '1a9c'),
                                  {'trees_count': 42}))

    await replicate(source=server_db, target=jane_db)
    await replicate(source=server_db, target=bob_db)

    print(next(jane_db.read_sync('roadside')))
    print(next(bob_db.read_sync('roadside')))

if __name__ == '__main__':
    import anyio
    anyio.run(main, backend='trio')
