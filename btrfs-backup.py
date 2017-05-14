#!/usr/bin/env python3

# Backup a btrfs volume to another, incrementally
# Requires Python >= 3.3, btrfs-progs >= 3.12 most likely.

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

import util
import endpoint


def send_snapshot(src_endpoint, dest_endpoint, sourcesnap, no_progress=False):
    logging.info(util.log_heading("Sending ..."))
    logging.info("  From:         {}".format(sourcesnap))
    logging.info("  To:           {}".format(dest_endpoint))

    # Now we need to send the snapshot (incrementally, if possible)
    latest_snapshot = src_endpoint.get_latest_snapshot()
    if latest_snapshot:
        logging.info("  Using parent: {}".format(latest_snapshot))

    pv = False
    if not no_progress:
        # check whether pv is available
        try:
            subprocess.check_output(['pv', '--help'])
        except (FileNotFoundError, subprocess.CalledProcessError):
            pass
        else:
            pv = True

    pipes = []
    pipes.append(src_endpoint.send(sourcesnap, parent=latest_snapshot))

    if pv:
        cmd = ["pv"]
        pipes.append(subprocess.Popen(cmd, stdin=pipes[-1].stdout,
                                      stdout=subprocess.PIPE))

    pipes.append(dest_endpoint.receive(pipes[-1].stdout))

    pids = [pipe.pid for pipe in pipes]
    while pids:
        pid, result = os.wait()
        if pid in pids:
            pids.remove(pid)
        if result != 0:
            logging.error("Error during btrfs send / receive")
            raise util.AbortError()


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="incremental btrfs backup",
                                     formatter_class=util.ArgparseSmartFormatter)
    parser.add_argument('-v', '--verbosity', default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        help="set verbosity level")
    parser.add_argument('-q', '--quiet', action='store_true',
                        help="shortcut for --no-progress --verbosity warning")
    parser.add_argument('-d', '--btrfs-debug', action='store_true',
                        help="enable debugging on btrfs send / receive")
    parser.add_argument('-P', '--no-progress', action='store_true',
                        help="don't display progress during backup")
    parser.add_argument('-C', '--skip-fs-checks', action='store_true',
                        help="don't check whether source / destination is a "
                             "btrfs subvolume / filesystem")
    parser.add_argument('-w', '--convert-rw', action='store_true',
                        help="convert read-only snapshots to read-write "
                             "before deleting them; allows regular users "
                             "to delete subvolumes when mount option "
                             "user_subvol_rm_allowed is enabled")
    parser.add_argument('-s', '--sync', action='store_true',
                        help="run 'btrfs subvolume sync' after deleting "
                             "subvolumes")
    parser.add_argument('-l', '--latest-only', action='store_true',
                        help="only keep latest snapshot on source filesystem")
    parser.add_argument('-n', '--num-backups', type=int, default=0,
                        help="only keep latest n backups at destination; "
                             "this option is only supported for local storage")
    parser.add_argument('-f', '--snapshot-folder',
                        help="snapshot folder in source filesystem; "
                             "either relative to source or absolute")
    parser.add_argument('-p', '--snapshot-prefix',
                        help="prefix for snapshot names")
    parser.add_argument("--ssh-opt", action="append",
                        help="N|pass extra ssh_config options to ssh(1);\n"
                             "example: '--ssh-opt Cipher=aes256-ctr --ssh-opt "
                             "IdentityFile=/root/id_rsa'\n"
                             "would result in 'ssh -o Cipher=aes256-ctr "
                             "-o IdentityFile=/root/id_rsa'")
    parser.add_argument("source", help="Subvolume to backup.")
    parser.add_argument("dest",
                        help="N|Destination to send backups to.\n"
                             "The following schemes are possible:\n"
                             " - /path/to/backups\n"
                             " - 'shell://cat > some-file'\n"
                             " - ssh://[user@]host[:port]/path/to/backups")
    args = parser.parse_args()

    if args.quiet:
        args.no_progress = True
        args.verbosity = "warning"

    logging.basicConfig(format="%(asctime)s  [%(levelname)-5s]  %(message)s",
                        datefmt="%H:%M:%S",
                        level=getattr(logging, args.verbosity.upper()))

    logging.info(util.log_heading("Started at {}".format(time.ctime())))

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
    logging.debug("Keep latest snapshot only: {}".format(args.latest_only))
    logging.debug("Number of backups to keep: {}".format(
        args.num_backups if args.num_backups > 0 else "Any"))

    # kwargs that are common between all endpoints
    endpoint_kwargs = {"snapprefix": snapprefix}

    src_path = os.path.abspath(args.source)
    logging.debug("Source: {}".format(src_path))
    src_endpoint = endpoint.LocalEndpoint(
        path=src_path,
        snapdir=snapdir,
        btrfs_debug=args.btrfs_debug,
        fstype_check=False,
        subvol_check=not args.skip_fs_checks,
        **endpoint_kwargs)

    # parse destination string
    dest = args.dest
    if dest.startswith("shell://"):
        dest_type = "shell"
        dest_cmd = dest[8:]
    elif dest.startswith("ssh://"):
        dest_type = "ssh"
        parsed_dest = urllib.parse.urlparse(dest)
        dest_path = parsed_dest.path
        if parsed_dest.query:
            dest_path += "?" + parsed_dest.query
    else:
        dest_type = "local"
        dest_path = dest

    logging.debug("Destination type: {}".format(dest_type))
    logging.debug("Destination: {}".format(dest))
    if dest_type == "shell":
        dest_endpoint = endpoint.ShellEndpoint(cmd=dest_cmd, **endpoint_kwargs)
    elif dest_type == "ssh":
        dest_endpoint = endpoint.SSHEndpoint(
            username=parsed_dest.username,
            hostname=parsed_dest.hostname,
            port=parsed_dest.port or 22,
            path=dest_path,
            ssh_opts=args.ssh_opt,
            btrfs_debug=args.btrfs_debug,
            **endpoint_kwargs)
    else:
        dest_endpoint = endpoint.LocalEndpoint(
            path=dest_path,
            btrfs_debug=args.btrfs_debug,
            fstype_check=not args.skip_fs_checks,
            subvol_check=False,
            **endpoint_kwargs)

    src_endpoint.prepare()
    dest_endpoint.prepare()

    # First we need to create a new snapshot on the source disk
    sourcesnap = src_endpoint.snapshot()

    # Need to sync
    src_endpoint.sync()

    send_snapshot(src_endpoint, dest_endpoint, sourcesnap,
                  no_progress=args.no_progress)
    logging.info(util.log_heading("Backup complete!"))

    src_endpoint.set_latest_snapshot(sourcesnap)

    logging.info(util.log_heading("Cleaning up ..."))
    # delete all but latest snapshot
    if args.latest_only:
        src_endpoint.delete_old_snapshots(1, convert_rw=args.convert_rw)
    # cleanup backups > NUM_BACKUPS in backup target
    if args.num_backups > 0:
        dest_endpoint.delete_old_backups(args.num_backups,
                                         convert_rw=args.convert_rw)

    # run 'btrfs subvolume sync'
    if args.sync:
        logging.info(util.log_heading("Syncing subvolumes ..."))
        src_endpoint.subvolume_sync()
        dest_endpoint.subvolume_sync()

    logging.info(util.log_heading("Finished at {}".format(time.ctime())))


if __name__ == "__main__":
    try:
        main()
    except (util.AbortError, KeyboardInterrupt):
        sys.exit(1)
