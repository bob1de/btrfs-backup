btrfs-backup
============

About
-----
This script supports incremental backups for ``btrfs`` using ``snapshots``
and ``send/receive`` between filesystems.  Think of it as a really basic
version of Time Machine.

Backups can be stored either locally or remotely (e.g. via SSH).

Its main goals are to be **reliable** and **functional** while maintaining
**user-friendliness**. It should be easy to get started in just a few
minutes without detailled knowledge on how ``btrfs send/receive``
works. However, you should have a basic understanding of snapshots
and subvolumes.

It is a fork of https://github.com/lordsutch/btrfs-backup with extended
features and some fixes.


Features
--------
* Initial creation of full backups using ``btrfs send / receive``
* Incremental backups on subsequent runs
* Storing backups locally, including deletion of old ones if desired
* Alternatively, the output of ``btrfs send`` may be piped to a custom
  command, what allows for storing backups on a remote server (e.g. via
  SSH)
* Creation of backups without root privileges, if some special conditions
  are met
* Detailled logging output with configurable log level


Requirements
------------
* Python 3.3 or later
* Appropriate btrfs-progs; typically you'll want **at least** 3.12 with
  Linux 3.12/3.13
* (optional) ``pv`` command for displaying progress during backups


Sample usage
------------
(as root)

	$ btrfs-backup.py /home /backup

This will create a read-only snapshot of ``/home``
in ``/home/snapshot/YYMMDD-HHMMSS``, and then send it to
``/backup/YYMMDD-HHMMSS``. On future runs, it will take a new read-only
snapshot and send the difference between the previous snapshot (tracked
with the symbolic link ``.latest``) and the new one.

**Note: Both source and destination filesystems need to be ``btrfs``
volumes.**

For the backup to be sensible, they shouldn't be the same filesystem.
Otherwise you could just snapshot and save the hassle.

You can backup multiple volumes to multiple subfolders or subvolumes on the
destination.  For example, you might want to backup both ``/`` and ``/home``.
The main caveat is you'll want to put the backups in separate folders
on the destination drive to avoid confusion.

	$ btrfs-backup.py / /backup/root
	$ btrfs-backup.py /home /backup/home

If you really want to store backups of different subvolumes at the same
location, you have to specify a prefix using the ``--snapshot-prefix``
option. Without that, ``btrfs-backup`` can't distinguish between your
different backup chains and will mix them up. Using the example from
above, it could look like the following:

	$ btrfs-backup.py --snapshot-prefix root / /backup
	$ btrfs-backup.py --snapshot-prefix home /home /backup

You can specify the flag ``--latest-only`` to only keep the most recent
snapshot on the source filesystem. The parameter ``--num-backups <num>``
tells ``btrfs-backup`` to delete all but the latest ``<num>``
backups. This one may only be used when backing up locally.


Backing up regularly
--------------------
With anacron on Debian, you could simply add a file ``/etc/cron.daily/local-backup``:

```sh
#!/bin/sh
ionice -c 3 /path/to/btrfs-backup.py --quiet --latest-only --num-backups 2 \
            /home /backup/home
```

More or less frequent backups could be made using other ``cron.*`` scripts.


Restoring a snapshot
--------------------
If necessary, you can restore a whole snapshot by using e.g.

	$ mkdir /home/snapshot
	$ btrfs send /backup/YYMMDD-HHMMSS | btrfs receive /home/snapshot

Then you need to take the read-only snapshot and turn it back into a
root filesystem:

	$ cp -aR --reflink /home/snapshot/YYMMDD-HHMMSS /home

You might instead have some luck taking the restored snapshot and turning it
into a read-write snapshot, and then re-pivoting your mounted
subvolume to the read-write snapshot.


Locking
-------
There is no locking. If you back up too often (i.e. more quickly than
it takes to make a snapshot, which can take several minutes on a
filesystem with lots of files), you might end up with a new backup
starting while an old one is in progress.

You can workaround the lack of locking using the ``flock(1)`` command, as
suggested at https://github.com/lordsutch/btrfs-backup/issues/4. For
example, in ``/etc/cron.hourly/local-backup``:

```sh
#!/bin/sh
flock -n /tmp/btrfs-backup.lock \
    ionice -c 3 /path/to/btrfs-backup.py --quiet --latest-only --num-backups 2 \
                /home /backup/home
```

You may omit the ``-n`` parameter if you want to wait rather than fail
in case a backup is already running.


Alternative workflow
--------------------
An alternative structure is to keep all subvolumes in the root directory

	/
	/active
	/active/root
	/active/home
	/inactive
	/snapshot/root/YYMMDD-HHMMSS
	/snapshot/home/YYMMDD-HHMMSS

and have corresponding entries in ``/etc/fstab`` to mount the subvolumes
from ``/active/*``. One benefit of this approach is that restoring
a snapshot can be done entirely with btrfs tools:

	$ btrfs send /backup/root/YYMMDD-HHMMSS | btrfs receive /snapshot/home
	$ btrfs send /backup/home/YYMMDD-HHMMSS | btrfs receive /snapshot/root
	$ mv /active/root /inactive
	$ mv /active/home /inactive
	$ btrfs subvolume snapshot /snapshot/root/YYMMDD-HHMMSS /active/root
	$ btrfs subvolume snapshot /snapshot/home/YYMMDD-HHMMSS /active/home

The snapshots from btrfs-backup may be placed in /snapshot-subvol/ by
using the --snapshot-dir option. Here is a simple backup script using
this approach:

```bash
#!/usr/bin/env bash

cmd=/usr/local/bin/btrfs-backup.py
destdir=/media/backup # Mount backup disk here
dirs=( '/' '/home' )  # Backup these dirs
btrfs0=/run/btrfs     # Mount subvolid=0 here
nbackup=24            # Keep this many backups on $destdir

for d in "${dirs[@]}"; do
    args="--num-backups $nbackup --snapshot-folder $btrfs0/snapshot_subvol$d --latest-only"
    echo Executing $cmd $args $d $destdir$d
    $cmd $args $d $destdir$d
done
```
