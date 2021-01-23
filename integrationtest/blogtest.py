import sys
sys.path.insert(0, '..')

from chairdb import InMemoryDatabase, replicate

async def main():
    server_db = InMemoryDatabase()
    jane_db = InMemoryDatabase()
    bob_db = InMemoryDatabase()

    server_db.write_sync({'_id': 'roadside', '_rev': '1-1a9c', 'trees_count': 40})
    await replicate(source=server_db, target=jane_db)
    await replicate(source=server_db, target=bob_db)

    bob_db.write_sync({'_id': 'roadside', '_rev': '2-e3b0', 'trees_count': 41, '_revisions': {'start': 2, 'ids': ['e3b0', '1a9c']}})
    jane_db.write_sync({'_id': 'roadside', '_rev': '2-6e05', 'trees_count': 41, '_revisions': {'start': 2, 'ids': ['6e05', '1a9c']}})
    await replicate(source=jane_db, target=server_db)
    await replicate(source=bob_db, target=server_db)

    for doc in server_db.read_sync('roadside', 'all', include_path=True):
        print(doc)

    fix1 = {'_id': 'roadside', '_rev': '3-b617', '_deleted': True,
            '_revisions': {'start': 3, 'ids': ['b617', '6e05', '1a9c']}}
    fix2 = {'_id': 'roadside', '_rev': '3-5bd6', 'trees_count': 42,
            '_revisions': {'start': 3, 'ids': ['5bd6', 'e3b0', '1a9c']}}
    server_db.write_sync(fix1)
    server_db.write_sync(fix2)

    await replicate(source=server_db, target=jane_db)
    await replicate(source=server_db, target=bob_db)

    print(next(jane_db.read_sync('roadside', 'winner')))
    print(next(bob_db.read_sync('roadside', 'winner')))

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
