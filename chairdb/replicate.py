import email.utils
import hashlib
import uuid

from .db.datatypes import NotFound

REPLICATION_ID_VERSION = 1


async def replicate(source, target, create_target=False, continuous=False):
    """Replicates the database 'source' to 'target', creating 'target' if
    necessary and 'create_target' is True according to CouchDB's replication
    protocol. Writes checkpoints to prevent unnecessary future work.

    https://docs.couchdb.org/en/stable/replication/protocol.html

    """
    hist_entry = {'session_id': uuid.uuid4().hex,
                  'start_time': timestamp(),
                  'doc_write_failures': 0,
                  'docs_read': 0}

    # 2.4.2.1. Verify Peers & 2.4.2.2. Get Peers Information
    #  2.4.2.1.1. Check Source Existence & 2.4.2.2.1. Get Source Information
    await source.update_seq

    #  2.4.2.1.2. Check Target Existence & 2.4.2.2.2. Get Target Information
    hist_entry['start_last_seq'] = await get_target_seq(target, create_target)

    # 2.4.2.3. Find Common Ancestry
    # - 2.4.2.3.1. Generate Replication ID
    replication_id = await gen_repl_id(source, target, create_target,
                                       continuous)

    # - 2.4.2.3.2. Retrieve Replication Logs from Source and Target
    source_log = await source.read_local(replication_id)
    target_log = await target.read_local(replication_id)

    # - 2.4.2.3.3. Compare Replication Logs
    startup_checkpoint = compare_replication_logs(source_log, target_log)
    hist_entry['recorded_seq'] = startup_checkpoint

    # 2.4.2.4. Locate Changed Documents
    # - 2.4.2.4.1. Listen to Changes Feed
    # - 2.4.2.4.2. Read Batch of Changes
    changes = source.changes(since=startup_checkpoint, continuous=continuous)
    diff_input = revs_diff_input(changes, hist_entry)

    # - 2.4.2.4.3. Calculate Revision Difference
    r_input = read_input(target.revs_diff(diff_input))

    # 2.4.2.5. Replicate Changes
    #  - 2.4.2.5.1. Fetch Changed Documents
    write_input = count_docs(source.read(r_input), hist_entry)

    # - 2.4.2.5.2. Upload Batch of Changed Documents
    # - 2.4.2.5.3. Upload Document with Attachments
    async for error in target.write(write_input):
        print(repr(error))
        hist_entry['doc_write_failures'] += 1

    # -  2.4.2.5.4. Ensure In Commit
    await target.ensure_full_commit()

    # - 2.4.2.5.5. Record Replication Checkpoint
    hist_entry['end_time'] = timestamp()
    write_count = hist_entry['docs_read'] - hist_entry['doc_write_failures']
    hist_entry['docs_written'] = write_count
    hist_entry['end_last_seq'] = hist_entry['recorded_seq']
    new_log_shared = {
        'replication_id_version': REPLICATION_ID_VERSION,
        'session_id': hist_entry['session_id'],
        'source_last_seq': hist_entry['recorded_seq'],
    }
    if hist_entry['recorded_seq'] != startup_checkpoint:
        new_source_log = {
            'history': build_history(source_log, hist_entry), **new_log_shared
        }
        new_target_log = {
            'history': build_history(target_log, hist_entry), **new_log_shared
        }

        await source.write_local(replication_id, new_source_log)
        await target.write_local(replication_id, new_target_log)

    # - 2.4.2.4.4. Replication Completed
    return {
        'ok': True,
        'history': [hist_entry],
        **new_log_shared,
    }


def timestamp():
    return email.utils.format_datetime(email.utils.localtime())


async def get_target_seq(target, create_target):
    try:
        return await target.update_seq
    except NotFound:
        # 2.4.2.1.3. Create Target?
        if create_target:
            await target.create()
            # second chance
            return await target.update_seq
        # 2.4.2.1.4. Abort
        raise


async def gen_repl_id(source, target, create_target, continuous):
    # 2.4.2.3.1. Generate Replication ID
    repl_id_values = ''.join([
        await source.id,
        await target.id,
        str(create_target),
        str(continuous),
    ]).encode('UTF-8')
    return hashlib.md5(repl_id_values).hexdigest()


def compare_replication_logs(source, target):
    # 2.4.2.3.3. Compare Replication Logs
    no_checkpoint = (
        # because there is no record of a previous replication
        source is None or target is None or
        # or because said replication happened under different (possibly buggy)
        # conditions
        source['replication_id_version'] != REPLICATION_ID_VERSION or
        target['replication_id_version'] != REPLICATION_ID_VERSION
    )
    if no_checkpoint:
        return
    if source['session_id'] == target['session_id']:
        return source['source_last_seq']  # shortcut

    # try to find commonality in diverging histories:
    session_ids = {item['session_id'] for item in source['history']}
    for item in target['history']:
        if item['session_id'] in session_ids:
            # found a previous shared session
            return item['recorded_seq']
    # no such luck: there's no known checkpoint.


async def revs_diff_input(changes, history_entry):
    async for change in changes:
        yield change.id, change.leaf_revs
        history_entry['recorded_seq'] = change.seq


async def read_input(differences):
    async for id, missing_revs, possible_ancestors in differences:
        yield id, {'revs': missing_revs, 'atts_since': possible_ancestors}


async def count_docs(docs, history_entry):
    async for doc in docs:
        if isinstance(doc, NotFound):
            continue  # required for skimdb. But how can this happen???
        history_entry['docs_read'] += 1
        yield doc


def build_history(existing_log, new_entry):
    try:
        existing_history = existing_log['history']
    except TypeError:
        return [new_entry]
    else:
        return [new_entry] + existing_history[:4]
