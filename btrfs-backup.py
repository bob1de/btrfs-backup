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

def delete_old_backups(backuploc, max_num_backups, snapshotprefix=''):
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
        backup_to_remove = os.path.join(backuploc, snapprefix +
                                        date2str(time_objs.pop(0)))
        print ("Removing old backup dir " + backup_to_remove)
        # delete snapshot of oldest backup snapshot
        delete_snapshot(backup_to_remove)

def delete_snapshot(snaploc):
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
    parser.add_argument('-l', '--latest-only', action='store_true',
                        help="only keep latest snapshot on source filesystem")
    parser.add_argument('-n', '--num-backups', type=int, default=0,
                        help="only keep latest n backups at destination")
    parser.add_argument('-s', '--snapshot-folder',
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

    if os.path.exists(args.source):
        sourceloc = os.path.abspath(args.source)
    else:
        print("backup source does not exist", file=sys.stderr)
        sys.exit(1)
    if not args.skip_fs_checks and not is_subvolume(sourceloc):
        print("backup source does not seem to be a btrfs subvolume")
        sys.exit(1)

    if args.dest_cmd:
        backuploc = args.dest
        NUM_BACKUPS = 0
    else:
        backuploc = os.path.abspath(args.dest)
        # Ensure backup directory exists
        if not os.path.exists(backuploc):
            try:
                os.makedirs(backuploc)
            except Exception as e:
                print("Error creating new backup location:", e, file=sys.stderr)
                sys.exit(1)
        if not args.skip_fs_checks and not is_btrfs(backuploc):
            print("Destination does not seem to be on a btrfs file system",
                  file=sys.stderr)
            sys.exit(1)
        NUM_BACKUPS = args.num_backups
        print("Num backups:", NUM_BACKUPS, file=sys.stderr)

    if args.snapshot_folder:
        SNAPSHOTDIR = args.snapshot_folder
    else:
        SNAPSHOTDIR = 'snapshot'
    if not SNAPSHOTDIR.startswith('/'):
        SNAPSHOTDIR = os.path.join(sourceloc, SNAPSHOTDIR)

    if args.snapshot_prefix:
        snapprefix = args.snapshot_prefix
        LASTNAME = '.' + snapprefix + '_latest'
    else:
        snapprefix = ''
        LASTNAME = '.latest'
    latest = os.path.join(SNAPSHOTDIR, LASTNAME)

    # Ensure snapshot directory exists
    snapdir = os.path.join(sourceloc, SNAPSHOTDIR)
    print("snapdir:", str(snapdir), file=sys.stderr)
    if not os.path.exists(snapdir):
        os.mkdir(snapdir)

    # First we need to create a new snapshot on the source disk
    sourcesnap = new_snapshot(sourceloc, snapdir, snapprefix)
    print("sourcesnap:", str(sourcesnap), file=sys.stderr)

    if not sourcesnap:
        print("snapshot failed", file=sys.stderr)
        sys.exit(1)

    # Need to sync
    cmd = ['sync']
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError:
        print("Error on command:", cmd, file=sys.stderr)

    print('Snapshot successful; sending backup', file=sys.stderr)
    print(' - from         ', sourcesnap, file=sys.stderr)
    if args.dest_cmd:
        print(' - receive cmd: ', backuploc, file=sys.stderr)
    else:
        print(' - to           ', backuploc, file=sys.stderr)

    # Now we need to send the snapshot (incrementally, if possible)
    real_latest = os.path.realpath(latest)
    if os.path.exists(real_latest):
        print(' - using parent:', real_latest, file=sys.stderr)
    else:
        real_latest = None

    result = send_snapshot(sourcesnap, backuploc, prevsnapshot=real_latest,
                           dest_cmd=args.dest_cmd, debug=args.debug)
    if result != 0:
        print("Error during btrfs send / receive, aborting", file=sys.stderr)
        sys.exit(1)

    if real_latest is not None and args.latest_only:
        print('Removing old snapshot', real_latest, file=sys.stderr)
        delete_snapshot(real_latest)

    if os.path.islink(latest):
        os.unlink(latest)
    elif os.path.exists(latest):
        print('confusion:', latest, "should be a symlink", file=sys.stderr)

    # Make .latest point to this backup - use relative symlink
    print('Latest snapshot now at', sourcesnap, file=sys.stderr)
    os.symlink(os.path.basename(sourcesnap), latest)
    print('Backup complete', file=sys.stderr)

    # cleanup backups > NUM_BACKUPS in backup target
    if not args.dest_cmd:
        if (NUM_BACKUPS > 0):
            delete_old_backups(backuploc, NUM_BACKUPS, snapprefix)
