import pushy
import notmuch as notmuch_local
import pushy.protocol.proxy
from ConfigParser import ConfigParser
from time import sleep
import os as os_local

conf = ConfigParser(allow_no_value=True)
conf.read(os_local.path.expanduser('~/.notmuch-pushy-config'))

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


def sync_msg_files(db_dest, msg_src, copy_file):
    filenames = [str(f) for f in msg_src.get_filenames()]
    first_msg_dest = None
    for f in filenames:
        copy_file(f)
    for f in filenames:
        (msg_dest, status) = db_dest.add_message(f)
        if first_msg_dest:
            assert msg_dest == first_msg_dest
        else:
            first_msg_dest = msg_dest
    assert msg_dest.get_message_id() == msg_src.get_message_id()
    return msg_dest


def sync_msg(msg_src, db_dest, copy_file, sync_tag, os_dest):
    print msg_src
    msg_id = msg_src.get_message_id()
    msg_dest = db_dest.find_message(msg_id)
    if msg_dest:
        dest_tags = [unicode(tag) for tag in msg_src.get_tags()]
        src_filenames = [str(f) for f in msg_src.get_filenames()]
        dest_filenames = [str(f) for f in msg_dest.get_filenames()]
        if u'learn-spam' in dest_tags or u'learn-ham' in dest_tags:
            assert len(dest_filenames) == 1 and len(src_filenames) == 1
            os_dest.remove(dest_filenames[0])
            for f in src_filenames:
                copy_file(f)
        elif len(src_filenames) > len(dest_filenames):
            for f in src_filenames:
                copy_file(f)
    else:
        msg_dest = sync_msg_files(db_dest, msg_src, copy_file)
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


def mk_copy_file(os, transfer_file):
    def copy_file(f):
        if not os.path.exists(f):
            transfer_file(f, f)
    return copy_file


def sync():
    db_local = get_db_rw_retry(notmuch_local)
    db_remote = get_db_rw_retry(notmuch_remote)

    local_msgs = get_messages_to_sync(db_local, local_sync_tag)
    remote_msgs = get_messages_to_sync(db_remote, remote_sync_tag)

    check_no_two_way_sync(local_msgs, remote_msgs)

    for msg in local_msgs:
        copy_file = mk_copy_file(os_remote, conn.putfile)
        sync_msg(msg, db_remote, copy_file, local_sync_tag, os_remote)
    for msg in remote_msgs:
        copy_file = mk_copy_file(os_local, conn.getfile)
        sync_msg(msg, db_local, copy_file, remote_sync_tag, os_local)

    db_local.close()
    db_remote.close()

if __name__ == '__main__':
    if any_changes():
        sync()
    else:
        print 'no change'
