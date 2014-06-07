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

parser = argparse.ArgumentParser(description="incremental btrfs backup")
parser.add_argument('--latest-only', action='store_true',
                    help="only keep latest snapshot on source filesystem")
parser.add_argument('-d', '--debug', action='store_true',
                    help="enable btrfs debugging on send/receive")
parser.add_argument('--snapshot-folder',
                    help="snapshot folder in source filesystem")
parser.add_argument('source', help="filesystem to backup")
parser.add_argument('backup', help="destination to send backups to")
args = parser.parse_args()

sourceloc = args.source
backuploc = args.backup

if args.snapshot_folder:
    SNAPSHOTDIR = args.snapshot_folder
else:
    SNAPSHOTDIR = 'snapshot'

LASTNAME = os.path.join(SNAPSHOTDIR, '.latest')


def datestr(timestamp=None):
    if timestamp is None:
        timestamp = time.localtime()
    return time.strftime('%Y%m%d-%H%M%S', timestamp)

def new_snapshot(disk, snapshotdir, readonly=True):
    snaploc = os.path.join(snapshotdir, datestr())
    command = ['btrfs', 'subvolume', 'snapshot']
    if readonly:
        command += ['-r']
    command += [disk, snaploc]

    subprocess.check_call(command)
    if os.path.exists(snaploc):
        return snaploc
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

def delete_snapshot(snaploc):
    subprocess.check_output(('btrfs', 'subvolume', 'delete', snaploc))

# Ensure snapshot directory exists
snapdir = os.path.join(sourceloc, SNAPSHOTDIR)
if not os.path.exists(snapdir):
    os.mkdir(snapdir)

# First we need to create a new snapshot on the source disk
sourcesnap = new_snapshot(sourceloc, snapdir)

if not sourcesnap:
    print("snapshot failed", file=sys.stderr)
    sys.exit(1)

# Need to sync
subprocess.check_call(['sync'])

# Now we need to send the snapshot (incrementally, if possible)
latest = os.path.join(sourceloc, LASTNAME)
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
