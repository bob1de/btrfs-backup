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

import subprocess
import sys
import os
import time
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

def new_snapshot(disk, snapshotdir, snapshotprefix, readonly=True):
    snapname = snapshotprefix + date2str()
    snaploc = os.path.join(snapshotdir, snapname)
    command = ['btrfs', 'subvolume', 'snapshot']
    if readonly:
        command += ['-r']
    command += [disk, snaploc]

    try:
        subprocess.check_call(command)
        return snaploc
    except subprocess.CalledProcessError:
        print("Error on command:", str(command), file=sys.stderr)
        return None

def send_snapshot(srcloc, destloc, prevsnapshot=None, debug=False):
    if debug:
        flags = ['-vv']
    else:
        flags = []

    srccmd = ['btrfs', 'send'] + flags
    if prevsnapshot:
        srccmd += ['-p', prevsnapshot]
    srccmd += [srcloc]

    destcmd = ['btrfs', 'receive'] + flags + [destloc]

    #print(srccmd)
    #print(destcmd)

    pipe = subprocess.Popen(srccmd, stdout=subprocess.PIPE)
    output = subprocess.check_output(destcmd, stdin=pipe.stdout)
    pipe.wait()
    #print(pipe.returncode, file=sys.stderr)
    return pipe.returncode

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
    subprocess.check_output(('btrfs', 'subvolume', 'delete', snaploc))

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="incremental btrfs backup")
    parser.add_argument('--latest-only', action='store_true',
                        help="only keep latest snapshot on source filesystem")
    parser.add_argument('-d', '--debug', action='store_true',
                        help="enable btrfs debugging on send/receive")
    parser.add_argument('--num-backups', type=int, default=0,
                        help="only store given number of backups in backup folder")
    parser.add_argument('--snapshot-folder',
                        help="snapshot folder in source filesystem")
    parser.add_argument('--snapshot-prefix',
                        help="prefix of snapshot name")
    parser.add_argument('source', help="filesystem to backup")
    parser.add_argument('backup', help="destination to send backups to")
    args = parser.parse_args()

    #This does not include a test if the source is a subvolume. It should be and this should be tested.
    if os.path.exists(args.source):
        sourceloc = args.source
    else:
        print("backup source subvolume does not exist", file=sys.stderr)
        sys.exit(1)

    #This does not include a test if the destination is a subvolume. It should be and this should be tested.
    if os.path.exists(args.backup):
        backuploc = args.backup
    else:
        print("backup destination subvolume does not exist", file=sys.stderr)
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

    # Ensure backup directory exists
    if not os.path.exists(backuploc):
        try:
            os.makedirs(backuploc)
        except:
            print("error creating new backup location:", str(backuploc), file=sys.stderr)
            sys.exit(1)

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
    subprocess.check_call(['sync'])

    # Now we need to send the snapshot (incrementally, if possible)
    real_latest = os.path.realpath(latest)

    if os.path.exists(real_latest):
        print('snapshot successful; sending incremental backup from', sourcesnap,
            'to', backuploc, 'using base', real_latest, file=sys.stderr)
        send_snapshot(sourcesnap, backuploc, real_latest, debug=args.debug)
        if args.latest_only:
            print('removing old snapshot', real_latest, file=sys.stderr)
            delete_snapshot(real_latest)
    else:
        print('snapshot successful; sending backup from', sourcesnap,
            'to', backuploc, file=sys.stderr)
        send_snapshot(sourcesnap, backuploc, debug=args.debug)

    if os.path.islink(latest):
        os.unlink(latest)
    elif os.path.exists(latest):
        print('confusion:', latest, "should be a symlink", file=sys.stderr)

    # Make .latest point to this backup - use relative symlink
    print('new snapshot at', sourcesnap, file=sys.stderr)
    os.symlink(os.path.basename(sourcesnap), latest)
    print('backup complete', file=sys.stderr)

    # cleanup backups > NUM_BACKUPS in backup target
    if (NUM_BACKUPS > 0):
        delete_old_backups(backuploc, NUM_BACKUPS, snapprefix)
