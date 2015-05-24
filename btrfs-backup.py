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

def datestr(timestamp=None):
    if timestamp is None:
        timestamp = time.localtime()
    return time.strftime('%Y%m%d-%H%M%S', timestamp)

def new_snapshot(disk, snapshotdir, snapshotprefix, readonly=True):
    snaploc = os.path.join(snapshotdir, snapshotprefix + datestr())
    command = ['btrfs', 'subvolume', 'snapshot']
    if readonly:
        command += ['-r']
    command += [disk, snaploc]

    try:
        subprocess.check_call(command)
        return snaploc
    except CalledProcessError:
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

def find_old_backup(bak_dir_time_objs,recurse_val = 0):
    """ Find oldest time object in "bak_dir_time_objs" structure.
        recurse_val = 0 -> start with top entry "year", default
    """

    tmp = []
    for timeobj in bak_dir_time_objs:
        tmp.append(timeobj[recurse_val])

    min_val = min(tmp) # find minimum time value
    new_timeobj = []

    for timeobj in bak_dir_time_objs:
        if(timeobj[recurse_val] == min_val):
            new_timeobj.append(timeobj)

    if (len(new_timeobj) > 1):
        return find_old_backup(new_timeobj,recurse_val+1) # recursive call from year to minute
    else:
        return new_timeobj[0]

def delete_old_backups(backuploc, max_num_backups):
    """ Delete old backup directories in backup target folder based on their date.
        Warning: This function will delete btrfs snapshots in target folder based on the parameter
        max_num_backups!
    """

    # recurse target backup folder until "max_num_backups" is reached
    cur_num_backups = len(os.listdir(backuploc))
    for i in range(cur_num_backups - max_num_backups):

        # find all backup snapshots in directory and build time object list
        bak_dir_time_objs = []
        for directory in os.listdir(backuploc):
            try:
                bak_dir_time_objs.append(time.strptime(directory, '%Y%m%d-%H%M%S'))
            except:
                pass

        # find oldest directory object and mark to remove
        bak_dir_to_remove = datestr(find_old_backup(bak_dir_time_objs, 0))
        bak_dir_to_remove_path = os.path.join(backuploc, bak_dir_to_remove)
        print ("Removing old backup dir " + bak_dir_to_remove_path)

        # delete snapshot of oldest backup snapshot
        delete_snapshot(bak_dir_to_remove_path)

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

    if args.snapshot_prefix:
        snapprefix = args.snapshot_prefix
        latest = os.path.join(SNAPSHOTDIR, '.' + snapprefix + '_latest')
    else:
        snapprefix = ''
        LASTNAME = os.path.join(SNAPSHOTDIR, '.latest')
        latest = os.path.join(sourceloc, LASTNAME)

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

    # Make .latest point to this backup
    print('new snapshot at', sourcesnap, file=sys.stderr)
    os.symlink(sourcesnap, latest)
    print('backup complete', file=sys.stderr)

    # cleanup backups > NUM_BACKUPS in backup target
    if (NUM_BACKUPS > 0):
        delete_old_backups(backuploc,NUM_BACKUPS)
