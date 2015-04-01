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

    # btrfs-backup.py /home /backup

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

    # btrfs-backup.py / /backup/root
    # btrfs-backup.py /home /backup/home

You can specify the flag --latest-only to only keep the most recent
snapshot on the source filesystem.

Backing up regularly
====================

With anacron on Debian, I simply added a file /etc/cron.daily/local-backup:

    #!/bin/sh
    ionice -c 3 /path/to/btrfs-backup.py /home /backup/home

More or less frequent backups could be made using other cron.* scripts.

Restoring a snapshot
====================

If necessary, you can restore a whole snapshot by using e.g.

    # mkdir /home/snapshot
    # btrfs send /backup/YYMMDD-HHMMSS | btrfs receive /home/snapshot

Then you need to take the read-only snapshot and turn it back into a
root filesystem:

    # cp -aR --reflink /home/snapshot/YYMMDD-HHMMSS /home

You might instead have some luck taking the restored snapshot and turning it
into a read-write snapshot, and then re-pivoting your mounted
subvolume to the read-write snapshot.

Locking
=======

There is no locking. If you back up too often (i.e. more quickly than
it takes to make a snapshot, which can take several minutes on a
filesystem with lots of files), you might end up with a new backup
starting while an old one is in progress.

You can workaround the lack of locking using the flock(1) command, as
suggested at https://github.com/lordsutch/btrfs-backup/issues/4. For
example, in /etc/cron.hourly/local-backup:

    #!/bin/sh
    flock -n /tmp/btrfs-backup.lock ionice -c 3 /path/to/btrfs-backup.py /home /backup/home

You can omit the '-n' parameter if you want to wait rather than fail
in this circumstance.

Alternative workflow
====================

An alternative structure is to keep all subvolumes in the root directory

    /
    /active-subvol
    /active-subvol/root
    /active-subvol/home
    /snapshot-subvol/root/YYMMDD-HHMMSS
    /snapshot-subvol/home/YYMMDD-HHMMSS

and have corresponding entries in /etc/fstab to mount the subvolumes
from /active-subvols/. One benefit of this approach is that restoring
a snapshot can be done entirely with btrfs tools:

    # btrfs send /backup/root/YYMMDD-HHMMSS | btrfs receive /snapshot-subvol/home/
    # btrfs send /backup/home/YYMMDD-HHMMSS | btrfs receive /snapshot-subvol/root/
    # btrfs subvolume snapshot /snapshot-subvol/root/YYMMDD-HHMMSS /active-subvol/root
    # btrfs subvolume snapshot /snapshot-subvol/home/YYMMDD-HHMMSS /active-subvol/home

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
