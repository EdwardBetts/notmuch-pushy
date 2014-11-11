import pushy
import notmuch as notmuch_local
import pushy.protocol.proxy
from ConfigParser import ConfigParser
from time import sleep
import os

conf = ConfigParser(allow_no_value=True)
conf.read(os.path.expanduser('~/.notmuch-pushy-config'))

host = conf.get('remote', 'host')
conn = pushy.connect('ssh:' + host)
notmuch_remote = conn.modules.notmuch
os_remote = conn.modules.os
local_sync_tag = conf.get('tags', 'local') or 'sync-to-server'
remote_sync_tag = conf.get('tags', 'remote') or 'sync-to-laptop'


def get_db_rw(notmuch):
    return notmuch.Database(mode=notmuch.Database.MODE.READ_WRITE)


def get_db_rw_retry(notmuch):
    try:
        return get_db_rw(notmuch)
    except notmuch.errors.XapianError:
        sleep(5)
        return get_db_rw(notmuch)


def get_messages_to_sync(db, tag):
    return list(db.create_query('tag:' + tag).search_messages())


def msg_id_set(msgs):
    return {msg.get_message_id() for msg in msgs}


def check_no_two_way_sync(local_msgs, remote_msgs):
    local_msg_ids = msg_id_set(local_msgs)
    remote_msg_ids = msg_id_set(remote_msgs)

    # we don't simultaneous tag modification on the same message
    assert local_msg_ids.isdisjoint(remote_msg_ids)


def sync_msg_files(db_dest, msg_src, transfer_file):
    filenames = [str(f) for f in msg_src.get_filenames()]
    first_msg_dest = None
    for f in filenames:
        # TODO only copy file is required, check with os.path.exists
        transfer_file(f, f)
    for f in filenames:
        (msg_dest, status) = db_dest.add_message(f)
        if first_msg_dest:
            assert msg_dest == first_msg_dest
        else:
            first_msg_dest = msg_dest
    assert msg_dest.get_message_id() == msg_src.get_message_id()
    return msg_dest


def sync_msg(msg_src, db_dest, transfer_file, sync_tag):
    print msg_src
    msg_id = msg_src.get_message_id()
    msg_dest = (db_dest.find_message(msg_id)
        or sync_msg_files(db_dest, msg_src, transfer_file))
    msg_dest.freeze()
    msg_dest.remove_all_tags(False)
    tags = [unicode(tag) for tag in msg_src.get_tags()]
    for tag in tags:
        if tag == sync_tag:
            continue
        msg_dest.add_tag(tag, False)
    msg_dest.thaw()
    msg_src.remove_tag(sync_tag)


def any_changes():
    db_local = notmuch_local.Database()
    db_remote = notmuch_remote.Database()

    local_msgs = get_messages_to_sync(db_local, local_sync_tag)
    remote_msgs = get_messages_to_sync(db_remote, remote_sync_tag)

    changes = bool(local_msgs or remote_msgs)
    if changes:
        check_no_two_way_sync(local_msgs, remote_msgs)
    db_local.close()
    db_remote.close()

    return changes


def sync():
    db_local = get_db_rw_retry(notmuch_local)
    db_remote = get_db_rw_retry(notmuch_remote)

    local_msgs = get_messages_to_sync(db_local, local_sync_tag)
    remote_msgs = get_messages_to_sync(db_remote, remote_sync_tag)

    check_no_two_way_sync(local_msgs, remote_msgs)

    for msg in local_msgs:
        sync_msg(msg, db_remote, conn.putfile, local_sync_tag)
    for msg in remote_msgs:
        sync_msg(msg, db_local, conn.getfile, remote_sync_tag)

    db_local.close()
    db_remote.close()

if __name__ == '__main__':
    if any_changes():
        sync()
    else:
        print 'no change'
