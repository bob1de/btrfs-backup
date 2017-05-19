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
import urllib.parse

from . import util
from . import endpoint


def send_snapshot(snapshot, dest_endpoint, parent=None, clones=None,
                  no_progress=False):
    # Now we need to send the snapshot (incrementally, if possible)
    logging.info(util.log_heading("Transferring {}".format(snapshot)))
    if parent:
        logging.info("Using parent: {}".format(parent))
    else:
        logging.info("No parent snapshot available, sending full backup.")
    if clones:
        logging.info("Using clones: {}".format(clones))

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


def sync_snapshots(src_endpoint, dest_endpoint, keep_num_backups=0, **kwargs):
    logging.info(util.log_heading("Transferring to {} "
                                  "...".format(dest_endpoint)))

    src_snapshots = src_endpoint.list_snapshots()
    dest_snapshots = dest_endpoint.list_snapshots()
    dest_endpoint_id = dest_endpoint.get_id()

    logging.debug("Planning transmissions ...")
    to_consider = src_snapshots
    if keep_num_backups > 0:
        # It wouldn't make sense to transfer snapshots that would be deleted
        # afterwards anyway.
        to_consider = to_consider[-keep_num_backups:]

    to_transfer = []
    for snapshot in to_consider:
        # filter source snapshots for only those already transferred
        if snapshot in dest_snapshots:
            # already transferred
            continue
        to_transfer.append(snapshot)

    if not to_transfer:
        logging.info("No snapshots need to be transferred.")
        return

    logging.info("Going to transfer {} snapshot(s):".format(len(to_transfer)))
    for snapshot in to_transfer:
        logging.info("  {}".format(snapshot))

    while to_transfer:
        present_snapshots = [s for s in src_snapshots
                             if s in dest_snapshots and
                                dest_endpoint_id not in s.locks]
        # choose snapshot with smallest distance to its parent
        def key(s):
            p = s.find_parent(present_snapshots)
            if p is None:
                return 999999999
            d = src_snapshots.index(s) - src_snapshots.index(p)
            return -d if d < 0 else d
        best_snapshot = min(to_transfer, key=key)
        parent = best_snapshot.find_parent(present_snapshots)
        lock_id = dest_endpoint.get_id()
        src_endpoint.set_lock(best_snapshot, lock_id, True)
        try:
            send_snapshot(best_snapshot, dest_endpoint, parent=parent,
                          clones=present_snapshots, **kwargs)
        except util.SnapshotTransferError:
            logging.info("Keeping {} locked to prevent it from getting "
                         "removed.".format(best_snapshot))
        else:
            src_endpoint.set_lock(best_snapshot, lock_id, False)
            dest_endpoint.add_snapshot(best_snapshot)
            dest_snapshots = dest_endpoint.list_snapshots()
        to_transfer.remove(best_snapshot)

    logging.info(util.log_heading("Snapshot transfers complete!"))


def run():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Incremental btrfs backup",
                                     formatter_class=util.ArgparseSmartFormatter,
                                     add_help=False)

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

    group = parser.add_argument_group("Retention settings")
    group.add_argument("-N", "--num-snapshots", type=int, default=0,
                       help="Only keep latest n snapshots on source "
                            "filesystem.")
    group.add_argument("-n", "--num-backups", type=int, default=0,
                       help="Only keep latest n backups at destination. "
                            "This option is not supported for 'shell://' "
                            "storage.")
    group.add_argument("--ignore-locks", action="store_true",
                       help="Force the retention policy - causes snapshots "
                            "to be removed even when they are locked due to "
                            "transmission failures.")

    group = parser.add_argument_group("Snapshot settings")
    group.add_argument("--no-snapshot", action="store_true",
                       help="Don't take a new snapshot, just transfer "
                            "existing ones.")
    group.add_argument("-f", "--snapshot-folder",
                       help="Snapshot folder in source filesystem; "
                            "either relative to source or absolute. "
                            "Default is 'snapshot'.")
    group.add_argument("-p", "--snapshot-prefix",
                       help="Prefix for snapshot names. Default is ''.")

    group = parser.add_argument_group("Miscellaneous options")
    group.add_argument("-C", "--skip-fs-checks", action="store_true",
                       help="Don't check whether source / destination is a "
                            "btrfs subvolume / filesystem.")
    group.add_argument("-s", "--sync", action="store_true",
                       help="Run 'btrfs subvolume sync' after deleting "
                            "subvolumes.")
    group.add_argument("-w", "--convert-rw", action="store_true",
                       help="Convert read-only snapshots to read-write "
                            "before deleting them. This allows regular users "
                            "to delete subvolumes when mount option "
                            "user_subvol_rm_allowed is enabled.")
    group.add_argument("--ssh-opt", action="append",
                       help="N|Pass extra ssh_config options to ssh(1).\n"
                            "Example: '--ssh-opt Cipher=aes256-ctr --ssh-opt "
                            "IdentityFile=/root/id_rsa'\n"
                            "would result in 'ssh -o Cipher=aes256-ctr "
                            "-o IdentityFile=/root/id_rsa'.")

    # for backwards compatibility only
    group = parser.add_argument_group("Deprecated options",
                                      description="These options are available "
                                                  "for backwards compatibility "
                                                  "only and will be removed in "
                                                  "future versions. Please "
                                                  "stop using them.")
    group.add_argument("--latest-only", action="store_true",
                       help="Shortcut for '--num-snapshots 1'.")

    group = parser.add_argument_group("Source and destination")
    group.add_argument("source", help="Subvolume to backup.")
    group.add_argument("dest", nargs="*",
                       help="N|Destination to send backups to.\n"
                            "The following schemes are possible:\n"
                            " - /path/to/backups\n"
                            " - 'shell://cat > some-file'\n"
                            " - ssh://[user@]host[:port]/path/to/backups\n"
                            "You may use this argument multiple times to "
                            "transfer backups to multiple locations. "
                            "You may even omit it completely in what case "
                            "no snapshot is transferred at all. That allows, "
                            "for instance, for well-organized local "
                            "snapshotting without backing up.")

    args = parser.parse_args()

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
        snapdir = 'snapshot'
    logging.debug("Snapshot folder: {}".format(snapdir))

    if args.snapshot_prefix:
        snapprefix = args.snapshot_prefix
    else:
        snapprefix = ''
    logging.debug("Snapshot prefix: {}".format(
        args.snapshot_prefix if args.snapshot_prefix else None))

    logging.debug("Enable btrfs debugging: {}".format(args.btrfs_debug))
    logging.debug("Don't display progress: {}".format(args.no_progress))
    logging.debug("Skip filesystem checks: {}".format(args.skip_fs_checks))
    logging.debug("Convert subvolumes to read-write before deletion: "
                  "{}".format(args.convert_rw))
    logging.debug("Run 'btrfs subvolume sync' afterwards: {}".format(args.sync))
    logging.debug("Don't take a new snapshot: {}".format(args.no_snapshot))
    logging.debug("Number of snapshots to keep: {}".format(args.num_snapshots))
    logging.debug("Number of backups to keep: "
                  "{}".format(args.num_backups if args.num_backups > 0
                              else "Any"))
    logging.debug("Ignore locks when deleting snapshots: "
                  "{}".format(args.ignore_locks))
    logging.debug("Extra SSH config options: {}".format(args.ssh_opt))

    # kwargs that are common between all endpoints
    endpoint_kwargs = {"snapprefix": snapprefix}

    src = os.path.abspath(args.source)
    src_endpoint = endpoint.LocalEndpoint(
        path=snapdir,
        source=src,
        btrfs_debug=args.btrfs_debug,
        fs_checks=not args.skip_fs_checks,
        **endpoint_kwargs)
    logging.debug("Source: {}".format(src))
    logging.debug("Source endpoint: {}".format(src_endpoint))

    dest_endpoints = []
    for dest in args.dest:
        # parse destination string
        if dest.startswith("shell://"):
            dest_type = "shell"
            dest_cmd = dest[8:]
            dest_endpoint = endpoint.ShellEndpoint(cmd=dest_cmd, **endpoint_kwargs)
        elif dest.startswith("ssh://"):
            dest_type = "ssh"
            parsed_dest = urllib.parse.urlparse(dest)
            if not parsed_dest.hostname:
                logging.error("No hostname for SSH specified.")
                raise util.AbortError()
            try:
                port = parsed_dest.port
            except ValueError:
                # invalid literal for int ...
                port = None
            dest_path = parsed_dest.path.strip() or "/"
            if parsed_dest.query:
                dest_path += "?" + parsed_dest.query
            dest_path = os.path.normpath(dest_path)
            dest_endpoint = endpoint.SSHEndpoint(
                username=parsed_dest.username,
                hostname=parsed_dest.hostname,
                port=port,
                path=dest_path,
                ssh_opts=args.ssh_opt,
            btrfs_debug=args.btrfs_debug,
                **endpoint_kwargs)
        else:
            dest_type = "local"
            dest_path = dest
            dest_endpoint = endpoint.LocalEndpoint(
                path=dest_path,
                btrfs_debug=args.btrfs_debug,
                fs_checks=not args.skip_fs_checks,
                **endpoint_kwargs)
        dest_endpoints.append(dest_endpoint)
        logging.debug("Destination type: {}".format(dest_type))
        logging.debug("Destination: {}".format(dest))
        logging.debug("Destination endpoint: {}".format(dest_endpoint))

    logging.info(util.log_heading("Preparing endpoints ..."))
    src_endpoint.prepare()
    for dest_endpoint in dest_endpoints:
        dest_endpoint.prepare()

    if not args.no_snapshot:
        # First we need to create a new snapshot on the source disk
        logging.info(util.log_heading("Snapshotting ..."))
        src_endpoint.snapshot()

    for dest_endpoint in dest_endpoints:
        sync_snapshots(src_endpoint, dest_endpoint,
                       keep_num_backups=args.num_backups,
                       no_progress=args.no_progress)

    logging.info(util.log_heading("Cleaning up ..."))
    # cleanup snapshots > num_snapshots in snapdir
    if args.num_snapshots > 0:
        src_endpoint.delete_old_snapshots(args.num_snapshots,
                                          ignore_locks=args.ignore_locks,
                                          convert_rw=args.convert_rw,
                                          sync=args.sync)
    # cleanup backups > num_backups in backup target
    if args.num_backups > 0:
        for dest_endpoint in dest_endpoints:
            dest_endpoint.delete_old_snapshots(args.num_backups,
                                               convert_rw=args.convert_rw,
                                               sync=args.sync)

    logging.info(util.log_heading("Finished at {}".format(time.ctime())))


def main():
    try:
        run()
    except (util.AbortError, KeyboardInterrupt):
        sys.exit(1)


if __name__ == "__main__":
    main()
