btrfs-backup
============
This project supports incremental backups for *btrfs* using *snapshots*
and *send/receive* between filesystems. Think of it as a basic version
of Time Machine.

Backups can be stored locally and/or remotely (e.g. via SSH). Multi-target
setups are supported as well as dealing with transmission failures
(e.g. due to network outage).

Its main goals are to be **reliable** and **functional** while
maintaining **user-friendliness**. It should be easy to get started in
just a few minutes without detailled knowledge on how btrfs send/receive
works. However, you should have a basic understanding of snapshots and
subvolumes.

btrfs-backup has almost no dependencies and hence is well suited for
many kinds of setups with only minimal maintenance effort.

Originally, it started as a fork of a project with the same name,
written by Chris Lawrence. Since then, most of the code has been
refactored and many new features were added before this repository
has been transferred to me. Many thanks to Chris for his work.
The old code base has been tagged with ``legacy``. If, for any reason,
you want to continue using it and miss the new features, you can check
that out.

:Latest release: v0.3.0
:Downloads: http://pypi.python.org/pypi/btrfs_backup
:Source: https://github.com/efficiosoft/btrfs-backup
:Platforms: Linux >= 3.12, Python >= 3.3
:Keywords: backup, btrfs, snapshot, send, receive, ssh


Features
--------
-  Initial creation of full backups
-  Incremental backups on subsequent runs
-  Different backup storage engines:

   -  Local storage
   -  Remote storage via SSH
   -  Custom storage: Alternatively, the output of ``btrfs send`` may be
      piped to a custom shell command.

-  Multi-target support with tracking of which snapshots are missing at
   each location.
-  Retransmission on errors (e.g. due to network outage).
-  Simple and configurable retention policy for local and remote
   snapshots
-  Optionally, create snapshots without transferring them anywhere
   and vice versa.
-  Creation of backups without root privileges, if some special
   conditions are met
-  Detailled logging output with configurable log level


Installation
------------
Requirements
~~~~~~~~~~~~
-  Python 3.3 or later
-  Appropriate btrfs-progs; typically you'll want **at least** 3.12 with
   Linux 3.12/3.13
-  (optional) OpenSSH's ``ssh`` command for remote backup storage
-  (optional) ``pv`` command for displaying progress during backups

Install via PIP
~~~~~~~~~~~~~~~
The easiest way to get up and running is via PIP. If ``pip3`` is missing
on your system and you run a Debian-based distribution, simply install
it via:

::

    $ sudo apt-get install python3-pip python3-wheel

Then, you can fetch the latest version of btrfs-backup:

::

    $ sudo pip3 install btrfs_backup

Manual installation
~~~~~~~~~~~~~~~~~~~
Alternatively, clone this git repository

::

    $ git clone https://github.com/efficiosoft/btrfs-backup
    $ cd btrfs-backup
    $ git checkout tags/v0.3.0  # optionally checkout a specific version
    $ sudo ./setup.py install


Sample usage
------------
Not every feature of btrfs-backup is explained in this README, since
there is a detailled and descriptive help included with the command.

However, there are some sections about the general concepts and different
sample usages to get started as quick as possible.

For reference, a copy of the output of ``btrfs-backup --help`` is
attached below.

As root:

::

    $ btrfs-backup /home /backup

This will create a read-only snapshot of ``/home`` in
``/home/snapshot/YYMMDD-HHMMSS``, and then send it to
``/backup/YYMMDD-HHMMSS``. On future runs, it will take a new read-only
snapshot and send the difference between the previous snapshot and the
new one.

**Note: Both source and destination need to be on btrfs filesystems.
Additionally, the source has to be either the root or any other subvolume,
but not just an ordinary directory because snapshots can only be created
of subvolumes.**

For the backup to be sensible, source and destination shouldn't be the
same filesystem. Otherwise you could just snapshot and save the hassle.

You can backup multiple subvolumes to multiple subfolders or subvolumes at
the destination. For example, you might want to backup both ``/`` and
``/home``. The main caveat is you'll want to put the backups in separate
folders on the destination drive to avoid confusion.

::

    $ btrfs-backup / /backup/root
    $ btrfs-backup /home /backup/home

If you really want to store backups of different subvolumes at the same
location, you have to specify a prefix using the ``-p/--snapshot-prefix``
option. Without that, btrfs-backup can't distinguish between your
different backup chains and will mix them up. Using the example from
above, it could look like the following:

::

    $ btrfs-backup --snapshot-prefix root / /backup
    $ btrfs-backup --snapshot-prefix home /home /backup

You can specify ``-N/--num-snapshots <num>`` to only keep the latest
``<num>`` number of snapshots on the source filesystem. ``-n/--num-backups
<num>`` does the same thing for the backup location.

Remote backups
~~~~~~~~~~~~~~
Backing up to a remote server via SSH is as easy as:

::

    $ btrfs-backup /home ssh://server/mnt/backups

btrfs-backup doesn't need to be installed on the remote side for this
to work. It is recommended to set up public key authentication to
eliminate the need for entering passwords. A full description of how
to customize the ``ssh`` call can be found in the help text.

Pulling backups from a remote SSH side is not yet supported. Please push
until it is.


Help text
---------
This is the output of ``btrfs-backup --help``. Taking a look at it,
you should get a good insight in what it can and can't do (yet).

::

    Cooming at the release.


What are locks?
---------------
btrfs-backup uses so called "locks" to keep track of failed snapshot
transfers. There is a file called ``.outstanding_transfers`` created in
the snapshot folder. This file is in JSON format and thus human-readable,
if necessary.

Locking works as follows:

#. When a snapshot transfer is started, an entry is created in that file,
   telling that a snapshot transfer of a specific snapshot to a specific
   destination has begun. We call this entry a lock.
#. When the transfer

   #. finishes without errors, the lock is removed.
   #. aborts (e.g. due to network outage or a full disk), the lock
      is kept.

Now, there are multiple options for dealing with those failed transfers.

When you run btrfs-backup the next time, it finds the corrupt snapshot
at the destination and deletes it, together with the corresponding lock.
Afterwards, the way is free for a new transfer. You may also use
``--no-snapshot`` to only do the transfers without creating new snapshots.

There is a special flag called ``--locked-dests`` available. If supplied,
it automatically adds all destinations which locks exist for as if they
were specified at the command line. You might do something like:

::

    $ btrfs-backup --no-snapshot --locked-dests /home

to retry all failed backup transfers of snapshots of ``/home``. This
could be executed periodically because it just does nothing if there
are no locks.

As a last resort for removing locks for transfers you don't want to retry
anymore, there is a flag called ``--remove-locks``. Use it with caution
and only if you can assure that there are no corrupt snapshots at the
destinations you apply the flag on.

::

    $ btrfs-backup --no-snapshot --no-transfer --remove-locks /home ssh://nas/backups

will remove all locks for the destination ``ssh://nas/backups`` from
``/home/snapshot/.outstanding_transfers``. Of course, using
``--locked-dests`` instead of specifying the destination explicitly is
possible as well.


Configuration files
-------------------
By default, btrfs-backup doesn't read any configuration file. However,
you can create one or more and specify them at the command line:

::

    $ btrfs-backup @path/to/backup_home.conf

Any argument prefixed by a ``@`` is treated as file name of a
configuration file.

The format of these files is simple. On every line, there may be one flag,
option or argument you would normally specify at the command line. Valid
configuration files might look like the following.

``backup_home.conf``:

::

    # This is a comment and thus ignored, as well as blank lines.

    # Include another configuration file here.
    @global.conf

            # Indentation has no effect.
            -p home

    # This is the source.
    /home

    # Back up to both local and remote storage.
    /mnt/backups/home
    ssh://server/mnt/btrfs_storage/backups/home

``global.conf``:

::

    # This file gets included by the other one.
    --quiet

    --num-snapshots 1
    --num-backups 3

A more detailled explanation about the format can be found in the help
text.


Backing up regularly
--------------------
Note that there is no locking included with btrfs-backup. If you back
up too often (i.e. more quickly than it takes the first call to finish,
which can take several minutes, hours or even days on a filesystem with
lots of files), you might end up with a new backup starting while an
old one is still in progress.

You can workaround the lack of locking using the ``flock(1)`` command,
as suggested at https://github.com/efficiosoft/btrfs-backup/issues/4.

With anacron on Debian, you could simply add a file
``/etc/cron.daily/local-backup``:

.. code:: sh

    #!/bin/sh
    flock -n /tmp/btrfs-backup-home.lock \
        ionice -c 3 btrfs-backup --quiet --num-snapshots 1 --num-backups 3 \
                    /home /backup/home

You may omit the ``-n`` flag if you want to wait rather than fail in
case a backup is already running.

More or less frequent backups could be made using other ``cron.*``
scripts.


Restoring a snapshot
--------------------
If necessary, you can restore a whole snapshot by using e.g.

::

    $ mkdir /home/snapshot
    $ btrfs send /backup/YYMMDD-HHMMSS | btrfs receive /home/snapshot

Then you need to take the read-only snapshot and turn it back into a
root filesystem:

::

    $ cp -aR --reflink /home/snapshot/YYMMDD-HHMMSS /home

You might instead have some luck taking the restored snapshot and
turning it into a read-write snapshot, and then re-pivoting your mounted
subvolume to the read-write snapshot.


Alternative workflow
--------------------
An alternative structure is to keep all subvolumes in the root directory

::

    /
    /active
    /active/root
    /active/home
    /inactive
    /snapshot/root/YYMMDD-HHMMSS
    /snapshot/home/YYMMDD-HHMMSS

and have corresponding entries in ``/etc/fstab`` to mount the subvolumes
from ``/active/*``. One benefit of this approach is that restoring a
snapshot can be done entirely with btrfs tools:

::

    $ btrfs send /backup/root/YYMMDD-HHMMSS | btrfs receive /snapshot/home
    $ btrfs send /backup/home/YYMMDD-HHMMSS | btrfs receive /snapshot/root
    $ mv /active/root /inactive
    $ mv /active/home /inactive
    $ btrfs subvolume snapshot /snapshot/root/YYMMDD-HHMMSS /active/root
    $ btrfs subvolume snapshot /snapshot/home/YYMMDD-HHMMSS /active/home

The snapshots from btrfs-backup may be placed in ``/snapshots/`` by
using the ``--snapshot-folder`` option.


Issues and Contribution
-----------------------
As in every piece of software, there likely are bugs. When you find one,
please open an issue on GitHub. If you do so, please include the output
with debug log level (``-v debug``) and provide steps to reproduce
the problem. Thank you!

If you want to contribute, that's great! You can create issues (even
for feature requests), send pull requests or contact me via email at
r.schindler@efficiosoft.com.


Copyright
---------
.. |copy|   unicode:: U+000A9 .. COPYRIGHT SIGN
| Copyright |copy| 2017 Robert Schindler <r.schindler@efficiosoft.com>  
| Copyright |copy| 2014 Chris Lawrence <lawrencc@debian.org>  
