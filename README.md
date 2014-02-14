btrfs-backup
============

This script supports basic incremental backups for btrfs using
snapshots and send/receive between filesystems.  Think of it as a
really basic version of Time Machine.

Requirements:

* Python 3.3 or later.
* Appropriate btrfs-progs; typically you'll want 3.12 with Linux 3.12/3.13.

Sample usage
============

(as root)

    \# btrfs-backup.py /home /backup

This will create a read-only snapshot in /home/snapshot/YYMMDD-HHMMSS,
and then send it to /backup/YYMMDD-HHMMSS. On future runs, it will
take a new read-only snapshot and send the difference between the
previous snapshot (tracked with the symbolic link ".latest") and the
new one.

Both source and destination filesystems need to be btrfs volumes. For
the backup to be sensible, they shouldn't be the same filesystem
(otherwise, why not just snapshot and save the hassle?).

You can backup multiple volumes to multiple subfolders or subvolumes on the
destination.  For example, you might want to backup both / and /home.
The main caveat is you'll want to put the backups in separate folders
on the destination drive to avoid confusion.

    \# btrfs-backup.py / /backup/root
    \# btrfs-backup.py /home /backup/home

You can specify the flag --latest-only to only keep the most recent
snapshot on the source filesystem.

Backing up regularly
====================

With anacron on Debian, I simply added a file /etc/cron.daily/local-backup:

    \#!/bin/sh
    ionice -c 3 /home /backup/home

More or less frequent backups could be made using other cron.* scripts.

Restoring a snapshot
====================

If necessary, you can restore a whole snapshot by using e.g.

    \# mkdir /home/snapshot
    \# btrfs send /backup/YYMMDD-HHMMSS | btrfs receive /home/snapshot

To use this as the base for future incremental backups:

    \# ln -s /home/snapshot/YYMMDD-HHMMSS /home/snapshot/.latest 

Then you need to take the read-only snapshot and turn it back into a
root filesystem:

    \# cp -aR --reflink /home/snapshot/YYMMDD-HHMMSS /home

Caveat
======

There is no locking. If you back up too often (i.e. more quickly than
it takes to make a snapshot, which can take several minutes on a
filesystem with lots of files), you might end up with a new backup
starting while an old one is in progress. Unsure whether adding
locking is worth the extra hassle.
