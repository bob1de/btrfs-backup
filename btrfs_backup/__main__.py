# Backup a btrfs volume to another, incrementally
# Requires Python >= 3.3, btrfs-progs >= 3.12 most likely.
#
# Copyright (c) 2017 Robert Schindler <r.schindler@efficiosoft.com>
# Copyright (c) 2014 Chris Lawrence <lawrencc@debian.org>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import sys
import os
import time
import subprocess
import logging
import argparse

from . import util
from . import endpoint


def send_snapshot(snapshot, dest_endpoint, parent=None, clones=None,
                  no_progress=False):
    """
    Sends snapshot to destination endpoint, using given parent and clones.
    It connects the pipes of source and destination together and shows
    progress data using the pv command.
    """

    # Now we need to send the snapshot (incrementally, if possible)
    logging.info("Sending {} ...".format(snapshot))
    if parent:
        logging.info("  Using parent: {}".format(parent))
    else:
        logging.info("  No parent snapshot available, sending in full mode.")
    if clones:
        logging.info("  Using clones: {}".format(clones))

    pv = False
    if not no_progress:
        # check whether pv is available
        logging.debug("Checking for pv ...")
        cmd = ["pv", "--help"]
        logging.debug("Executing: {}".format(cmd))
        try:
            subprocess.call(cmd, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
        except FileNotFoundError as e:
            logging.debug("  -> got exception: {}".format(e))
            logging.debug("  -> pv is not available")
        else:
            logging.debug("  -> pv is available")
            pv = True

    pipes = []
    pipes.append(snapshot.endpoint.send(snapshot, parent=parent, clones=clones))

    if pv:
        cmd = ["pv"]
        logging.debug("Executing: {}".format(cmd))
        pipes.append(subprocess.Popen(cmd, stdin=pipes[-1].stdout,
                                      stdout=subprocess.PIPE))

    pipes.append(dest_endpoint.receive(pipes[-1].stdout))

    pids = [pipe.pid for pipe in pipes]
    while pids:
        pid, retcode = os.wait()
        if pid in pids:
            logging.debug("  -> PID {} exited with return code "
                          "{}".format(pid, retcode))
            pids.remove(pid)
        if retcode != 0:
            logging.error("Error during btrfs send / receive")
            raise util.SnapshotTransferError()


def sync_snapshots(src_endpoint, dest_endpoint, keep_num_backups=0,
                   no_incremental=False, **kwargs):
    """
    Synchronizes snapshots from source to destination. Takes care
    about locking and deletion of corrupt snapshots from failed transfers.
    It never transfers snapshots that would anyway be deleted afterwards
    due to retention policy.
    """

    logging.info(util.log_heading("  To {} ...".format(dest_endpoint)))

    src_snapshots = src_endpoint.list_snapshots()
    dest_snapshots = dest_endpoint.list_snapshots()
    dest_id = dest_endpoint.get_id()

    # delete corrupt snapshots from destination
    to_remove = []
    for snapshot in src_snapshots:
        if snapshot in dest_snapshots and dest_id in snapshot.locks:
            # seems to have failed previously and is present at
            # destination; delete corrupt snapshot there
            dest_snapshot = dest_snapshots[dest_snapshots.index(snapshot)]
            logging.info("Potentially corrupt snapshot {} found at "
                         "{}".format(dest_snapshot, dest_endpoint))
            to_remove.append(dest_snapshot)
    if to_remove:
        dest_endpoint.delete_snapshots(to_remove)
        # refresh list of snapshots at destination to have deleted ones
        # disappear
        dest_snapshots = dest_endpoint.list_snapshots()
    # now that deletion worked, remove all locks for this destination
    for snapshot in src_snapshots:
        if dest_id in snapshot.locks:
            src_endpoint.set_lock(snapshot, dest_id, False)
        if dest_id in snapshot.parent_locks:
            src_endpoint.set_lock(snapshot, dest_id, False, parent=True)

    logging.debug("Planning transmissions ...")
    to_consider = src_snapshots
    if keep_num_backups > 0:
        # it wouldn't make sense to transfer snapshots that would be deleted
        # afterwards anyway
        to_consider = to_consider[-keep_num_backups:]
    to_transfer = [snap for snap in to_consider if snap not in dest_snapshots]

    if not to_transfer:
        logging.info("No snapshots need to be transferred.")
        return

    logging.info("Going to transfer {} snapshot(s):".format(len(to_transfer)))
    for snapshot in to_transfer:
        logging.info("  {}".format(snapshot))

    while to_transfer:
        if no_incremental:
            # simply choose the last one
            best_snapshot = to_transfer[-1]
            parent = None
            clones = []
        else:
            # pick the snapshots common among source and dest,
            # exclude those that had a failed transfer before
            present_snapshots = [s for s in src_snapshots
                                 if s in dest_snapshots and
                                    dest_id not in s.locks]
            # choose snapshot with smallest distance to its parent
            def key(s):
                p = s.find_parent(present_snapshots)
                if p is None:
                    return 999999999
                d = src_snapshots.index(s) - src_snapshots.index(p)
                return -d if d < 0 else d
            best_snapshot = min(to_transfer, key=key)
            parent = best_snapshot.find_parent(present_snapshots)
            # we don't use clones at the moment, because they don't seem
            # to speed things up
            #clones = present_snapshots
            clones = []
        src_endpoint.set_lock(best_snapshot, dest_id, True)
        if parent:
            src_endpoint.set_lock(parent, dest_id, True, parent=True)
        try:
            send_snapshot(best_snapshot, dest_endpoint, parent=parent,
                          clones=clones, **kwargs)
        except util.SnapshotTransferError:
            logging.info("Keeping {} locked to prevent it from getting "
                         "removed.".format(best_snapshot))
        else:
            src_endpoint.set_lock(best_snapshot, dest_id, False)
            if parent:
                src_endpoint.set_lock(parent, dest_id, False, parent=True)
            dest_endpoint.add_snapshot(best_snapshot)
            dest_snapshots = dest_endpoint.list_snapshots()
        to_transfer.remove(best_snapshot)

    logging.info(util.log_heading("Transfers to {} "
                                  "complete!".format(dest_endpoint)))


def run(argv):
    """Run the program. Items in ``argv`` are treated as command line
       arguments."""

    description = """\
This provides incremental backups for btrfs filesystems. It can be
used for taking regular backups of any btrfs subvolume and syncing them
with local and/or remote locations. Multiple targets are supported as
well as retention settings for both source snapshots and backups. If
a snapshot transfer fails for any reason (e.g. due to network outage),
btrfs-backup will notice it and prevent the snapshot from being deleted
until it finally maked it over to its destination."""

    epilog = """\
You may also pass one or more file names prefixed with '@' at the
command line. Arguments are then read from these files, treating each
line as a flag or '--arg value'-style pair you would normally
pass directly. Note that you must not escape whitespaces (or anything
else) within argument values. Lines starting with '#' are treated
as comments and silently ignored. Blank lines and indentation are allowed
and have no effect. Argument files can be nested, meaning you may include
a file from another one. When doing so, make sure to not create infinite
loops by including files mutually. Mixing of direct arguments and argument
files is allowed as well."""

    # Parse command line arguments
    parser = util.MyArgumentParser(description=description, epilog=epilog,
                                   add_help=False, fromfile_prefix_chars="@",
                                   formatter_class=util.MyHelpFormatter)

    group = parser.add_argument_group("Display settings")
    group.add_argument("-h", "--help", action="help",
                       help="Show this help message and exit.")
    group.add_argument("-v", "--verbosity", default="info",
                       choices=["debug", "info", "warning", "error"],
                       help="Set verbosity level. Default is 'info'.")
    group.add_argument("-q", "--quiet", action="store_true",
                       help="Shortcut for '--no-progress --verbosity "
                            "warning'.")
    group.add_argument("-d", "--btrfs-debug", action="store_true",
                       help="Enable debugging on btrfs send / receive.")
    group.add_argument("-P", "--no-progress", action="store_true",
                       help="Don't display progress and stats during backup.")

    group = parser.add_argument_group("Retention settings",
                                      description="By default, snapshots are "
                                                  "kept forever at both source "
                                                  "and destination. With these "
                                                  "settings you may specify an "
                                                  "alternate retention policy.")
    group.add_argument("-N", "--num-snapshots", type=int, default=0,
                       help="Only keep latest n snapshots on source "
                            "filesystem.")
    group.add_argument("-n", "--num-backups", type=int, default=0,
                       help="Only keep latest n backups at destination. "
                            "This option is not supported for 'shell://' "
                            "storage.")

    group = parser.add_argument_group("Snapshot creation settings")
    group.add_argument("-S", "--no-snapshot", action="store_true",
                       help="Don't take a new snapshot, just transfer "
                            "existing ones.")
    group.add_argument("-f", "--snapshot-folder",
                       help="Snapshot folder in source filesystem; "
                            "either relative to source or absolute. "
                            "Default is 'snapshot'.")
    group.add_argument("-p", "--snapshot-prefix",
                       help="Prefix for snapshot names. Default is ''.")

    group = parser.add_argument_group("Transfer related options")
    group.add_argument("-T", "--no-transfer", action="store_true",
                       help="Don't transfer any snapshot.")
    group.add_argument("-I", "--no-incremental", action="store_true",
                       help="Don't ever try to send snapshots incrementally. "
                            "This might be useful when piping to a file for "
                            "storage.")

    group = parser.add_argument_group("SSH related options")
    group.add_argument("--ssh-opt", action="append", default=[],
                       help="N|Pass extra ssh_config options to ssh(1).\n"
                            "Example: '--ssh-opt Cipher=aes256-ctr --ssh-opt "
                            "IdentityFile=/root/id_rsa'\n"
                            "would result in 'ssh -o Cipher=aes256-ctr "
                            "-o IdentityFile=/root/id_rsa'.")
    group.add_argument("--ssh-sudo", action="store_true",
                       help="Execute commands with sudo on the remote host.")

    group = parser.add_argument_group("Miscellaneous options")
    group.add_argument("-s", "--sync", action="store_true",
                       help="Run 'btrfs subvolume sync' after deleting "
                            "subvolumes.")
    group.add_argument("-w", "--convert-rw", action="store_true",
                       help="Convert read-only snapshots to read-write "
                            "before deleting them. This allows regular users "
                            "to delete subvolumes when mount option "
                            "user_subvol_rm_allowed is enabled.")
    group.add_argument("--remove-locks", action="store_true",
                       help="Remove locks for all given destinations from all "
                            "snapshots present at source. You should only use "
                            "this flag if you can assure that no partially "
                            "transferred snapshot is left at any given "
                            "destination. It might be useful together with "
                            "'--no-snapshot --no-transfer --locked-dests' "
                            "in order to clean up any existing lock without "
                            "doing anything else.")
    group.add_argument("--skip-fs-checks", action="store_true",
                       help="Don't check whether source / destination is a "
                            "btrfs subvolume / filesystem. Normally, you "
                            "shouldn't need to use this flag. If it is "
                            "necessary in a working setup, please consider "
                            "filing a bug.")

    # for backwards compatibility only
    group = parser.add_argument_group("Deprecated options",
                                      description="These options are available "
                                                  "for backwards compatibility "
                                                  "only and might be removed "
                                                  "in future releases. Please "
                                                  "stop using them.")
    group.add_argument("--latest-only", action="store_true",
                       help="Shortcut for '--num-snapshots 1'.")

    group = parser.add_argument_group("Source and destination")
    group.add_argument("--locked-dests", action="store_true",
                       help="Automatically add all destinations for which "
                            "locks exist at any source snapshot.")
    group.add_argument("source",
                       help="N|Subvolume to backup.\n"
                            "The following schemes are possible:\n"
                            " - /path/to/subvolume\n"
                            " - ssh://[user@]host[:port]/path/to/subvolume\n"
                            "Specifying a source is mandatory.")
    group.add_argument("dest", nargs="*", default=[],
                       help="N|Destination to send backups to.\n"
                            "The following schemes are possible:\n"
                            " - /path/to/backups\n"
                            " - ssh://[user@]host[:port]/path/to/backups\n"
                            " - 'shell://cat > some-file'\n"
                            "You may use this argument multiple times to "
                            "transfer backups to multiple locations. "
                            "You may even omit it completely in what case "
                            "no snapshot is transferred at all. That allows, "
                            "for instance, for well-organized local "
                            "snapshotting without backing up.")

    try:
        args = parser.parse_args(argv)
    except RecursionError as e:
        print("Recursion error while parsing arguments.\n"
              "Maybe you produced a loop in argument files?", file=sys.stderr)
        raise util.AbortError()

    # applying shortcuts
    if args.quiet:
        args.no_progress = True
        args.verbosity = "warning"
    if args.latest_only:
        args.num_snapshots = 1

    logging.basicConfig(format="%(asctime)s  [%(levelname)-5s]  %(message)s",
                        datefmt="%H:%M:%S",
                        level=getattr(logging, args.verbosity.upper()))

    logging.info(util.log_heading("Started at {}".format(time.ctime())))

    logging.debug(util.log_heading("Settings"))
    if args.snapshot_folder:
        snapdir = args.snapshot_folder
    else:
        snapdir = "snapshot"

    if args.snapshot_prefix:
        snapprefix = args.snapshot_prefix
    else:
        snapprefix = ""

    logging.debug("Enable btrfs debugging: {}".format(args.btrfs_debug))
    logging.debug("Don't display progress: {}".format(args.no_progress))
    logging.debug("Don't take a new snapshot: {}".format(args.no_snapshot))
    logging.debug("Number of snapshots to keep: {}".format(args.num_snapshots))
    logging.debug("Number of backups to keep: "
                  "{}".format(args.num_backups if args.num_backups > 0
                              else "Any"))
    logging.debug("Snapshot folder: {}".format(snapdir))
    logging.debug("Snapshot prefix: "
                  "{}".format(snapprefix if snapprefix else None))
    logging.debug("Don't transfer snapshots: {}".format(args.no_transfer))
    logging.debug("Don't send incrementally: {}".format(args.no_incremental))
    logging.debug("Extra SSH config options: {}".format(args.ssh_opt))
    logging.debug("Use sudo at SSH remote host: {}".format(args.ssh_sudo))
    logging.debug("Run 'btrfs subvolume sync' afterwards: {}".format(args.sync))
    logging.debug("Convert subvolumes to read-write before deletion: "
                  "{}".format(args.convert_rw))
    logging.debug("Remove locks for given destinations: "
                  "{}".format(args.remove_locks))
    logging.debug("Skip filesystem checks: {}".format(args.skip_fs_checks))
    logging.debug("Auto add locked destinations: {}".format(args.locked_dests))

    # kwargs that are common between all endpoints
    endpoint_kwargs = {"snapprefix": snapprefix,
                       "convert_rw": args.convert_rw,
                       "subvolume_sync": args.sync,
                       "btrfs_debug": args.btrfs_debug,
                       "fs_checks": not args.skip_fs_checks,
                       "ssh_opts": args.ssh_opt,
                       "ssh_sudo": args.ssh_sudo}

    logging.debug("Source: {}".format(args.source))
    src_endpoint_kwargs = dict(endpoint_kwargs)
    src_endpoint_kwargs["path"] = snapdir
    try:
        src_endpoint = endpoint.choose_endpoint(args.source,
                                                src_endpoint_kwargs,
                                                source=True)
    except ValueError as e:
        logging.error("Couldn't parse source specification: {}".format(e))
        raise util.AbortError()
    logging.debug("Source endpoint: {}".format(src_endpoint))
    src_endpoint.prepare()

    # add endpoint creation strings for locked destinations, if desired
    if args.locked_dests:
        for snapshot in src_endpoint.list_snapshots():
            for lock in snapshot.locks:
                if lock not in args.dest:
                    args.dest.append(lock)

    if args.remove_locks:
        logging.info("Removing locks (--remove-locks) ...")
        for snapshot in src_endpoint.list_snapshots():
            for dest in args.dest:
                if dest in snapshot.locks:
                    logging.info("  {} ({})".format(snapshot, dest))
                    src_endpoint.set_lock(snapshot, dest, False)
                if dest in snapshot.parent_locks:
                    logging.info("  {} ({}) [parent]".format(snapshot, dest))
                    src_endpoint.set_lock(snapshot, dest, False, parent=True)

    dest_endpoints = []
    # only create destination endpoints if they are needed
    if args.no_transfer and args.num_backups <= 0:
        logging.debug("Don't creating destination endpoints because they "
                      "won't be needed (--no-transfer and no --num-backups).")
    else:
        for dest in args.dest:
            logging.debug("Destination: {}".format(dest))
            try:
                dest_endpoint = endpoint.choose_endpoint(dest, endpoint_kwargs,
                                                         source=False)
            except ValueError as e:
                logging.error("Couldn't parse destination specification: "
                              "{}".format(e))
                raise util.AbortError()
            dest_endpoints.append(dest_endpoint)
            logging.debug("Destination endpoint: {}".format(dest_endpoint))
            dest_endpoint.prepare()

    if args.no_snapshot:
        logging.info("Taking no snapshot (--no-snapshot).")
    else:
        # First we need to create a new snapshot on the source disk
        logging.info(util.log_heading("Snapshotting ..."))
        src_endpoint.snapshot()

    if args.no_transfer:
        logging.info(util.log_heading("Not transferring (--no-transfer)."))
    else:
        logging.info(util.log_heading("Transferring ..."))
        for dest_endpoint in dest_endpoints:
            try:
                sync_snapshots(src_endpoint, dest_endpoint,
                               keep_num_backups=args.num_backups,
                               no_incremental=args.no_incremental,
                               no_progress=args.no_progress)
            except util.AbortError as e:
                logging.error("Aborting snapshot transfer to {} due to "
                              " exception.".format(dest_endpoint))
                logging.debug("Exception was: {}".format(e))
        if not dest_endpoints:
            logging.info("No destination configured, don't sending anything.")

    logging.info(util.log_heading("Cleaning up ..."))
    # cleanup snapshots > num_snapshots in snapdir
    if args.num_snapshots > 0:
        try:
            src_endpoint.delete_old_snapshots(args.num_snapshots)
        except util.AbortError as e:
            logging.debug("Got AbortError while deleting source snapshots at "
                          "{}".format(src_endpoint))
    # cleanup backups > num_backups in backup target
    if args.num_backups > 0:
        for dest_endpoint in dest_endpoints:
            try:
                dest_endpoint.delete_old_snapshots(args.num_backups)
            except util.AbortError as e:
                logging.debug("Got AbortError while deleting backups at "
                              "{}".format(dest_endpoint))

    logging.info(util.log_heading("Finished at {}".format(time.ctime())))


def main():
    try:
        run(sys.argv[1:])
    except (util.AbortError, KeyboardInterrupt):
        sys.exit(1)


if __name__ == "__main__":
    main()
