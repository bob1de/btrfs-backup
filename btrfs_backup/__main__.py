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


def send_snapshot(snapshot, dest_endpoint, parent=None, no_progress=False):
    # Now we need to send the snapshot (incrementally, if possible)
    logging.info(util.log_heading("Transferring {}".format(snapshot)))
    logging.info("To:           {}".format(dest_endpoint))
    if parent:
        logging.info("Using parent: {}".format(parent))
    else:
        logging.info("No previous snapshot available, sending full backup.")

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
    pipes.append(snapshot.endpoint.send(snapshot, parent=parent))

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
            raise util.AbortError()


def sync_snapshots(src_endpoint, dest_endpoint, keep_num_backups=0, **kwargs):
    logging.info(util.log_heading("Transferring snapshots ..."))

    src_snapshots = src_endpoint.list_snapshots()
    dest_snapshots = dest_endpoint.list_snapshots()

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

    logging.info("Going to transfer {} snapshot(s):".format(len(to_transfer)))
    for snapshot in to_transfer:
        logging.info("  {}".format(snapshot))

    while to_transfer:
        present_snapshots = [s for s in src_snapshots
                             if s in dest_snapshots]
        # choose snapshot with smallest distance to its parent
        def key(s):
            p = s.find_parent(present_snapshots)
            if p is None:
                return 999999999
            d = src_snapshots.index(s) - src_snapshots.index(p)
            return -d if d < 0 else d
        best_snapshot = min(to_transfer, key=key)
        parent = best_snapshot.find_parent(present_snapshots)
        send_snapshot(best_snapshot, dest_endpoint, parent=parent, **kwargs)
        dest_endpoint.add_snapshot(best_snapshot)
        dest_snapshots = dest_endpoint.list_snapshots()
        to_transfer.remove(best_snapshot)

    logging.info(util.log_heading("Snapshot transfers complete!"))


def run():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Incremental btrfs backup",
                                     formatter_class=util.ArgparseSmartFormatter)
    parser.add_argument("-v", "--verbosity", default="info",
                        choices=["debug", "info", "warning", "error"],
                        help="set verbosity level")
    parser.add_argument("-q", "--quiet", action="store_true",
                        help="Shortcut for '--no-progress --verbosity "
                             "warning'.")
    parser.add_argument("-d", "--btrfs-debug", action="store_true",
                        help="Enable debugging on btrfs send / receive.")
    parser.add_argument("-P", "--no-progress", action="store_true",
                        help="Don't display progress and stats during backup.")
    parser.add_argument("-C", "--skip-fs-checks", action="store_true",
                        help="Don't check whether source / destination is a "
                             "btrfs subvolume / filesystem.")
    parser.add_argument("-w", "--convert-rw", action="store_true",
                        help="Convert read-only snapshots to read-write "
                             "before deleting them. This allows regular users "
                             "to delete subvolumes when mount option "
                             "user_subvol_rm_allowed is enabled.")
    parser.add_argument("-s", "--sync", action="store_true",
                        help="Run 'btrfs subvolume sync' after deleting "
                             "subvolumes.")
    parser.add_argument("-N", "--num-snapshots", type=int, default=0,
                        help="Only keep latest n snapshots on source "
                             "filesystem.")
    parser.add_argument("-n", "--num-backups", type=int, default=0,
                        help="Only keep latest n backups at destination. "
                             "This option is not supported for 'shell://' "
                             "storage.")
    parser.add_argument("--latest-only", action="store_true",
                        help="Shortcut for '--num-snapshots 1' (for backwards "
                             "compatibility).")
    parser.add_argument("-f", "--snapshot-folder",
                        help="Snapshot folder in source filesystem; "
                             "either relative to source or absolute.")
    parser.add_argument("-p", "--snapshot-prefix",
                        help="Prefix for snapshot names.")
    parser.add_argument("--ssh-opt", action="append",
                        help="N|Pass extra ssh_config options to ssh(1).\n"
                             "Example: '--ssh-opt Cipher=aes256-ctr --ssh-opt "
                             "IdentityFile=/root/id_rsa'\n"
                             "would result in 'ssh -o Cipher=aes256-ctr "
                             "-o IdentityFile=/root/id_rsa'.")
    parser.add_argument("source", help="Subvolume to backup.")
    parser.add_argument("dest",
                        help="N|Destination to send backups to.\n"
                             "The following schemes are possible:\n"
                             " - /path/to/backups\n"
                             " - 'shell://cat > some-file'\n"
                             " - ssh://[user@]host[:port]/path/to/backups")
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
    logging.debug("Convert subvolumes to read-write before deletion: {}".format(
        args.convert_rw))
    logging.debug("Run 'btrfs subvolume sync' afterwards: {}".format(args.sync))
    logging.debug("Number of snapshots to keep: {}".format(args.num_snapshots))
    logging.debug("Number of backups to keep: {}".format(
        args.num_backups if args.num_backups > 0 else "Any"))
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

    # parse destination string
    dest = args.dest
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
    logging.debug("Destination type: {}".format(dest_type))
    logging.debug("Destination: {}".format(dest))
    logging.debug("Destination endpoint: {}".format(dest_endpoint))

    logging.info(util.log_heading("Preparing endpoints ..."))
    src_endpoint.prepare()
    dest_endpoint.prepare()

    # First we need to create a new snapshot on the source disk
    logging.info(util.log_heading("Snapshotting ..."))
    sourcesnap = src_endpoint.snapshot()

    sync_snapshots(src_endpoint, dest_endpoint,
                   keep_num_backups=args.num_backups,
                   no_progress=args.no_progress)

    logging.info(util.log_heading("Cleaning up ..."))
    # cleanup snapshots > num_snapshots in snapdir
    if args.num_snapshots > 0:
        src_endpoint.delete_old_snapshots(args.num_snapshots,
                                          convert_rw=args.convert_rw,
                                          sync=args.sync)
    # cleanup backups > num_backups in backup target
    if args.num_backups > 0:
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
