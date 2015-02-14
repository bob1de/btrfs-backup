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
previous snapshot (tracked with a symbolic link named
".latest.<TARGETNAME>.<SOURCE>") and the new one.

Both source and destination filesystems need to be btrfs volumes. For
the backup to be sensible, they shouldn't be the same filesystem
(otherwise, why not just snapshot and save the hassle?).

Local Backup
------------

You can backup multiple volumes to multiple subfolders or subvolumes
on the destination.  For example, you might want to backup /boot, /
and /home, preferred with one btrfs-backup.py call (to get the same
YYYYMMDD-HHMMSS datetime stamp for snapshots that belong together).
As the source path is used as part of the snapshot name, snapshots
from different partitions that belong together can be stored in the
same directory without troubles.

    # btrfs-backup.py --snapshot /boot/.snapshot --source /boot \
         --snapshot /mnt/.snapshot/TheHostName --source /mnt/@ \
         --source /mnt/@home \
         --backup /backup/snapshot/TheHostName

To support creating snapshots which are stored in different btrfs
filesystems (here: /boot and /mnt), the --snapshot parameter is used
to set the snapshot location for all --source parameters that follow.

The above command will create the following snapshots and symlinks:

    /boot/.snapshot/YYYYMMDD-HHMMSS-boot
    /boot/.snapshot/.latest.local.boot -> YYYYMMDD-HHMMSS-boot
    /mnt/.snapshot/TheHostName/YYYYMMDD-HHMMSS-@
    /mnt/.snapshot/TheHostName/YYYYMMDD-HHMMSS-@home
    /mnt/.snapshot/TheHostName/.latest.local.@ -> YYYYMMDD-HHMMSS-@
    /mnt/.snapshot/TheHostName/.latest.local.@home -> YYYYMMDD-HHMMSS-@home

Symlinks are named .latest.local.* as no --targetname is specified.
You can specify the flag --latest-only to only keep the most recent
snapshot on the source filesystem.

Remote Backup
-------------

btrfs-backup.py also allows sending the backup to a remote host via a custom
--remote_backup command:

    # btrfs-backup.py --snapshot /boot/.snapshot --source /boot \
         --remote_backup "['ssh', 'TheBackupUser@TheBackupHost',
             'cat > inComing/%DEST%.btrfs-send']" \
         --snapshot /mnt/.snapshot/TheHostName --source /mnt/@ \
         --source /mnt/@home \
         --targetname 'TheBackupHost'

Note that the --remote_backup command expects a python array specifying the
external command to be run, using "%DEST% as a placeholder for the destination
snapshot name that shall be created.
To distinguish the symlinks that are created, the --targetname parameter
is used, therefore this time the symlinks for keeping track which snapshot
was sent last to the remote host are named

    /boot/.snapshot/.latest.TheBackupHost.boot -> YYYYMMDD-HHMMSS-boot
    /mnt/.snapshot/thehostname/.latest.TheBackupHost.@ -> YYYYMMDD-HHMMSS-@
    /mnt/.snapshot/thehostname/.latest.TheBackupHost.@home -> YYYYMMDD-HHMMSS-@home

On TheBackupHost the btrfs send output is stored in the ~TheBackupUser/inComing
subdirectory, and can be re-assembled into btrfs snapshots via btrfs receive
there. By using that indirect approach no root permissions have to be given on
TheBackupHost to receive the snapshots.

Trial run
---------
To just display the (btrfs send, symlink update) commands that would be executed,
add the --trial parameter. This parameter is nice for setting up without always
changing symlinks.

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

Caveat
======

There is no locking. If you back up too often (i.e. more quickly than
it takes to make a snapshot, which can take several minutes on a
filesystem with lots of files), you might end up with a new backup
starting while an old one is in progress. Unsure whether adding
locking is worth the extra hassle.
