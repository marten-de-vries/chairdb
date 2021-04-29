import sys
sys.path.insert(0, '..')

from chairdb import InMemoryDatabase, replicate, Document, anext

async def main():
    server_db = InMemoryDatabase()
    jane_db = InMemoryDatabase()
    bob_db = InMemoryDatabase()

    await server_db.write(Document('roadside', 1, ('1a9c',), {'trees_count': 40}))
    await replicate(source=server_db, target=jane_db)
    await replicate(source=server_db, target=bob_db)

    await bob_db.write(Document('roadside', 2, ('e3b0', '1a9c'), {'trees_count': 41}))
    await jane_db.write(Document('roadside', 2, ('6e05', '1a9c'), {'trees_count': 41}))
    await replicate(source=jane_db, target=server_db)
    await replicate(source=bob_db, target=server_db)

    async for doc in server_db.read('roadside', revs='all'):
        print(doc)

    await server_db.write(Document('roadside', 3, ('b617', '6e05', '1a9c'),
                                   is_deleted=True))
    await server_db.write(Document('roadside', 3, ('5bd6', 'e3b0', '1a9c'),
                                   {'trees_count': 42}))

    await replicate(source=server_db, target=jane_db)
    await replicate(source=server_db, target=bob_db)

    print(await anext(jane_db.read('roadside')))
    print(await anext(bob_db.read('roadside')))

if __name__ == '__main__':
    import anyio
    anyio.run(main, backend='trio')
