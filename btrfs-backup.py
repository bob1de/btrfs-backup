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
import argparse


TIMEFORMAT = '%Y%m%d-%H%M%S'

def date2str(timestamp=None, format=None):
    if timestamp is None:
        timestamp = time.localtime()
    if format is None:
        format = TIMEFORMAT
    return time.strftime(format, timestamp)

def str2date(timestring=None, format=None):
    if timestring is None:
        return time.localtime()
    if format is None:
        format = TIMEFORMAT
    return time.strptime(timestring, format)

def is_btrfs(path):
    """Checks whether path is inside a btrfs file system"""
    path = os.path.normpath(os.path.abspath(path))
    best_match = ''
    best_match_fstype = ''
    for line in open('/proc/mounts'):
        try:
            mountpoint, fstype = line.split(' ')[1:3]
        except ValueError:
            continue
        if path.startswith(mountpoint) and len(mountpoint) > len(best_match):
            best_match = mountpoint
            best_match_fstype = fstype
    return best_match_fstype == 'btrfs'

def is_subvolume(path):
    """Checks whether the given path is a btrfs subvolume."""
    if not is_btrfs(path):
        return False
    # subvolumes always have inode 256
    st = os.stat(path)
    return st.st_ino == 256

def new_snapshot(disk, snapshotdir, snapshotprefix, readonly=True):
    snapname = snapshotprefix + date2str()
    snaploc = os.path.join(snapshotdir, snapname)
    cmd = ['btrfs', 'subvolume', 'snapshot']
    if readonly:
        cmd += ['-r']
    cmd += [disk, snaploc]

    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError:
        print("Error on command:", cmd, file=sys.stderr)
        return None
    return snaploc

def send_snapshot(src, dest, prevsnapshot=None, dest_cmd=False, debug=False):
    if debug:
        flags = ['-vv']
    else:
        flags = []

    srccmd = ['btrfs', 'send'] + flags
    if prevsnapshot:
        srccmd += ['-p', prevsnapshot]
    srccmd += [src]

    if dest_cmd:
        destcmd = dest
    else:
        destcmd = ['btrfs', 'receive'] + flags + [dest]

    # check whether pv is available
    try:
        subprocess.check_output(['pv', '--help'])
    except (FileNotFoundError, subprocess.CalledProcessError):
        pv = False
    else:
        pv = True

    pipe = subprocess.Popen(srccmd, stdout=subprocess.PIPE)
    if pv:
        pvcmd = ['pv']
        pipe = subprocess.Popen(pvcmd, stdin=pipe.stdout,
                                stdout=subprocess.PIPE)
    try:
        output = subprocess.check_call(destcmd, stdin=pipe.stdout,
                                       shell=dest_cmd)
    except subprocess.CalledProcessError:
        print("Error on command:", destcmd, file=sys.stderr)
        return None
    return pipe.wait()

def delete_old_backups(backuploc, max_num_backups, snapshotprefix='',
                       convert_rw=False):
    """ Delete old backup directories in backup target folder based on their date.
        Warning: This function will delete btrfs snapshots in target folder based on the parameter
        max_num_backups!
    """

    time_objs = []
    for item in os.listdir(backuploc):
        if os.path.isdir(os.path.join(backuploc, item)) and \
           item.startswith(snapshotprefix):
            time_str = item[len(snapshotprefix):]
            try:
                time_objs.append(str2date(time_str))
            except ValueError:
                # no valid name for current prefix + time string
                continue

    # sort by date, then time;
    time_objs.sort()

    while time_objs and len(time_objs) > max_num_backups:
        # delete oldest backup snapshot
        backup_to_remove = os.path.join(backuploc, snapprefix +
                                        date2str(time_objs.pop(0)))
        delete_snapshot(backup_to_remove, convert_rw=convert_rw)

def delete_snapshot(snaploc, convert_rw=False):
    if convert_rw:
        print("Converting snapshot to read-write:", snaploc)
        cmd = ['btrfs', 'property', 'set', '-ts', snaploc, 'ro', 'false']
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError:
            print("Error on command:", cmd, file=sys.stderr)
            return None
    print("Removing snapshot:", snaploc)
    cmd = ['btrfs', 'subvolume', 'delete', snaploc]
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError:
        print("Error on command:", cmd, file=sys.stderr)


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="incremental btrfs backup")
    parser.add_argument('-d', '--debug', action='store_true',
                        help="enable debugging on btrfs send / receive")
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
                        help="only keep latest n backups at destination")
    parser.add_argument('-f', '--snapshot-folder',
                        help="snapshot folder in source filesystem; "
                             "either relative to source or absolute")
    parser.add_argument('-p', '--snapshot-prefix',
                        help="prefix for snapshot names")
    parser.add_argument('-c', '--dest-cmd', action='store_true',
                        help="interpret the dest argument as a command for "
                             "receiving snapshots instead of a directory; "
                             "this option makes --num-backups ineffective")
    parser.add_argument('source', help="subvolume to backup")
    parser.add_argument('dest', help="destination to send backups to")
    args = parser.parse_args()

    print("-" * 80)

    source = os.path.abspath(args.source)
    print("Source:", source)
    if not os.path.exists(source):
        print("Backup source does not exist", file=sys.stderr)
        sys.exit(1)
    if not args.skip_fs_checks and not is_subvolume(source):
        print("Backup source does not seem to be a btrfs subvolume",
              file=sys.stderr)
        sys.exit(1)

    if args.dest_cmd:
        dest = args.dest
        print("Destination command:", dest)
    else:
        dest = os.path.abspath(args.dest)
        print("Destination:", dest)
        # Ensure backup directory exists
        if not os.path.exists(dest):
            try:
                os.makedirs(dest)
            except Exception as e:
                print("Error creating new backup location:", e, file=sys.stderr)
                sys.exit(1)
        if not args.skip_fs_checks and not is_btrfs(dest):
            print("Destination does not seem to be on a btrfs filesystem",
                  file=sys.stderr)
            sys.exit(1)

    if args.snapshot_folder:
        snapdir = args.snapshot_folder
    else:
        snapdir = 'snapshot'
    if not snapdir.startswith('/'):
        snapdir = os.path.join(source, snapdir)
    print("Snapshot folder:", snapdir)

    if args.snapshot_prefix:
        snapprefix = args.snapshot_prefix
        lastname = '.' + snapprefix + '_latest'
    else:
        snapprefix = ''
        lastname = '.latest'
    latest = os.path.join(snapdir, lastname)
    print("Snapshot prefix:",
          args.snapshot_prefix if args.snapshot_prefix else "None")

    print("Skip filesystem checks:", args.skip_fs_checks)
    print("Convert subvolumes to read-write before deletion:", args.convert_rw)
    print("Run 'btrfs subvolume sync' afterwards:", args.sync)
    print("Keep latest snapshot only:", args.latest_only)
    print("Number of backups to keep:",
          args.num_backups if args.num_backups > 0 else "Any")

    # Ensure snapshot directory exists
    if not os.path.exists(snapdir):
        try:
            os.makedirs(snapdir)
        except Exception as e:
            print("Error creating snapshot folder:", e, file=sys.stderr)
            sys.exit(1)

    print("-" * 80)

    # First we need to create a new snapshot on the source disk
    sourcesnap = new_snapshot(source, snapdir, snapprefix)
    if not sourcesnap:
        print("Snapshot failed", file=sys.stderr)
        sys.exit(1)

    # Need to sync
    print("Syncing disks ...")
    cmd = ['sync']
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError:
        print("Error on command:", cmd, file=sys.stderr)

    print("-" * 80)

    print("Sending backup:")
    print(" - from         ", sourcesnap)
    if args.dest_cmd:
        print(" - receive cmd: ", dest)
    else:
        print(" - to           ", dest)

    # Now we need to send the snapshot (incrementally, if possible)
    real_latest = os.path.realpath(latest)
    if os.path.exists(real_latest):
        print(" - using parent:", real_latest)
    else:
        real_latest = None

    result = send_snapshot(sourcesnap, dest, prevsnapshot=real_latest,
                           dest_cmd=args.dest_cmd, debug=args.debug)
    if result != 0:
        print("Error during btrfs send / receive", file=sys.stderr)
        sys.exit(1)

    print("-" * 80)
    print("Backup complete!")

    if os.path.islink(latest):
        os.unlink(latest)
    elif os.path.exists(latest):
        print("Confusion:", latest, "should be a symlink", file=sys.stderr)

    # Make .latest point to this backup - use relative symlink
    print("Latest snapshot now at", sourcesnap)
    os.symlink(os.path.basename(sourcesnap), latest)

    print("-" * 80)
    print("Cleaning up ...")

    if real_latest is not None and args.latest_only:
        delete_snapshot(real_latest, convert_rw=args.convert_rw)

    # cleanup backups > NUM_BACKUPS in backup target
    if not args.dest_cmd and args.num_backups > 0:
        delete_old_backups(dest, args.num_backups, snapprefix,
                           convert_rw=args.convert_rw)

    # run 'btrfs subvolume sync'
    if args.sync:
        print("-" * 80)
        locations = [source]
        if not args.dest_cmd:
            locations.append(dest)
        for location in locations:
            print("Running 'btrfs subvolume sync' for", location, "...")
            cmd = ['btrfs', 'subvolume', 'sync', location]
            try:
                subprocess.check_call(cmd)
            except subprocess.CalledProcessError:
                print("Error on command:", cmd, file=sys.stderr)

    print("-" * 80)
    print("Done!")
