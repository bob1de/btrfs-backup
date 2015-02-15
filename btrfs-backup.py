#!/usr/bin/env python3

# Backup btrfs volume(s) to another, incrementally
# Requires Python >= 3.3, btrfs-progs >= 3.12 most likely.

# Copyright (c) 2014 Chris Lawrence <lawrencc@debian.org>
# Modifications Copyright (c) 2014-2015 Klaus Holler <kho@gmx.at>
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

start_time = time.localtime()

def datestr(timestamp=None):
    if timestamp is None:
        timestamp = time.localtime()
    return time.strftime('%Y%m%d-%H%M%S', timestamp)

def new_snapshot(disk, snapshotdir, timestamp=start_time, readonly=True, trial=False):
    snaploc = os.path.join(snapshotdir, datestr(timestamp) + '-' + os.path.basename(disk))
    command = ['btrfs', 'subvolume', 'snapshot']
    if trial:
        command.insert(0, 'echo')
    if readonly:
        command += ['-r']
    command += [disk, snaploc]

    try:
        subprocess.check_call(command)
        if os.path.exists(snaploc):
            return snaploc
        if trial:
            return snaploc  # fake success
    except CalledProcessError:
        print("Error on command:", str(command), file=sys.stderr)
    return None

def send_snapshot(srcloc, destloc, prevsnapshot=None, debug=False, trial=False,
                  remote_backup_command=None):
    if debug:
        flags = ['-vv']
    else:
        flags = []

    srccmd = ['btrfs', 'send'] + flags
    if trial:
        srccmd.insert(0, 'echo')
    if prevsnapshot:
        srccmd += ['-p', prevsnapshot]
    srccmd += [srcloc]

    if remote_backup_command is not None:
        # custom remote backup command instead of normal btrfs receive %DEST%
        destcmd = [it.replace("%DEST%", os.path.basename(srcloc)) for it in remote_backup_command]
    else:
        destcmd = ['btrfs', 'receive'] + flags + [destloc]
    if trial:
        destcmd.insert(0, 'echo')

    print("  ", srccmd)
    print("  ", destcmd)

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

def delete_old_backups(backuploc, source_postfix, max_num_backups, trial):
    """ Delete old backup directories in backup target folder based on their date.
        Warning: This function will delete btrfs snapshots in target folder based on
        the parameter max_num_backups!
    """
    # As snapshots of different partitions/mountpoints can be stored in the same
    # directory now, have to filter only those that match the current source (postfix).
    # recurse target backup folder until "max_num_backups" is reached
    backups_of_source = [d for d in os.listdir(backuploc)
                            if d.endswith(source_postfix)]
    print(backups_of_source)
    cur_num_backups = len(backups_of_source)
    for i in range(cur_num_backups - max_num_backups):
        # find all backup snapshots in directory and build time object list
        bak_dir_time_objs = []
        backups_of_source = [d for d in os.listdir(backuploc)
                                if d.endswith(source_postfix)]
        for directory in backups_of_source:
            timestamp = re.sub(source_postfix, '', directory)
            bak_dir_time_objs.append(time.strptime(timestamp, '%Y%m%d-%H%M%S'))

        # find oldest directory object and mark to remove
        bak_dir_to_rm = datestr(find_old_backup(bak_dir_time_objs, 0)) + source_postfix
        bak_dir_to_rm_path = os.path.join(backuploc, bak_dir_to_rm)
        print ("Removing old backup dir " + bak_dir_to_rm_path)
        # delete snapshot of oldest backup snapshot
        delete_snapshot(bak_dir_to_rm_path, trial)

def delete_snapshot(snaploc, trial=False):
    delcmd = ['btrfs', 'subvolume', 'delete', snaploc]
    if trial:
        delcmd.insert(0, 'echo')
    delcmd.insert(0, 'echo')    # make sure that during testing snapshots are not deleted
    subprocess.check_output(delcmd)

if __name__ == "__main__":
    print("btrfs-backup started at %s." % time.asctime(start_time))

    source_to_snapshot = list()     # for every source remember corresponding snapshot_folder
    class SourceArgAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            #print('%r %r %r %r' % (namespace, values, option_string, namespace.snapshot_folder))
            tup = (values, namespace.snapshot_folder)
            source_to_snapshot.append(tup)

    parser = argparse.ArgumentParser(description="incremental btrfs backup (for multiple "
                                     " partitions, naming snapshots created together with the "
                                     " same base datetime stamp)")
    parser.add_argument('--latest-only', action='store_true',
                        help="only keep latest snapshot on source filesystem")
    parser.add_argument('-d', '--debug', action='store_true',
                        help="enable btrfs debugging on send/receive")
    parser.add_argument('--num-backups', type=int, default=0,
                        help="only store given number of backups in backup folder")
    parser.add_argument('--snapshot-folder', action='store', default=".snapshot",
                        help="snapshot folder in source filesystem")
    parser.add_argument('-t', '--trial', action='store_true',
                help="trial run: only show commands that would be executed, but don't run anything")
    # can be used multiple times e.g. -source /boot -source /mnt/@ -source /mnt/@home
    parser.add_argument('-s', '--source', action=SourceArgAction, help="filesystem(s) to backup")
    parser.add_argument('-b', '--backup', help="(local) destination directory to send backups to")
    parser.add_argument('-r', '--remote_backup', help="command to connect/run at destination host, using %DEST% as placeholder for destination snapshot filename (if needed)")
    parser.add_argument('-T', '--targetname', help="name of backup target, useful for creating multiple symlinks that point to the last backed up snapshot per target")
    args = parser.parse_args()

    backuploc = args.backup
    NUM_BACKUPS = args.num_backups
    print("Num backups:", NUM_BACKUPS, file=sys.stderr)

    trial = args.trial

    if (NUM_BACKUPS > 0):
        delete_old_backups(backuploc,NUM_BACKUPS)
        sys.exit(1)     # indicate success

    targetname = args.targetname
    if args.remote_backup is not None:
        if type(args.remote_backup) == "<class 'list'>":
            remote_backup_command = args.remote_backup
        elif type(args.remote_backup) == type('str'):
            if args.remote_backup.startswith('['):
                remote_backup_command = eval(args.remote_backup)
            else:
                remote_backup_command = [args.remote_backup]
        else:
            raise Exception('Sorry, but type %s of remote_backup is currently not supported' % type(args.remote_backup))
        print(" remote_backup command: ", remote_backup_command)
    else:
        remote_backup_command = None

    print(" SOURCES: ", source_to_snapshot)

    if trial:
        print("Trial run requested: only show commands that would be executed, but don't run anything")

    # First we need to create a new snapshot on the source disk(s) for all sources
    snapshots_to_backup = list()
    problems = list()
    for (sourceloc, snapdir) in source_to_snapshot:
        # Ensure snapshot directory exists
        # TODO: test if the source is a subvolume, it should be and this should be tested.
        if not os.path.exists(snapdir):
            problems.append("Snapshot base path %r for source %r does not exist, source skipped." % \
                (snapdir, sourceloc))
            continue
        sourcesnap = new_snapshot(sourceloc, snapdir, trial=trial)
        if not sourcesnap:
            problems.append("snapshot for %r to %r failed" % (sourceloc, snapdir))
        else:
            snapshots_to_backup.append((sourceloc, sourcesnap, snapdir))

    if len(problems) > 0:
        if trial:
            print("Trial: ignoring problems encountered while creating snapshots:\n * " + \
                  "\n * ".join(problems), file=sys.stderr)
        else:
            print("Problems encountered while creating snapshots:\n * " + \
                  "\n * ".join(problems), file=sys.stderr)
            sys.exit(1)

    # Need to sync
    synccmd = ['sync']
    if trial:
        synccmd.insert(0, 'echo')
    subprocess.check_call(synccmd)
    print('Creating snapshot(s) was successful.', file=sys.stderr)
    if (backuploc is None) and (remote_backup_command is None):
        print('Neither local backup location nor remote backup command specified, stopping now after creating snapshots.', file=sys.stderr)
        sys.exit(0)

    if backuploc is None:
        print('Going to send them via remote backup command:',
              " ".join(remote_backup_command), '...', file=sys.stderr)
        if targetname is None:
            targetname = 'remote'
    else:
        print('Going to send them to', backuploc, '...', file=sys.stderr)
        # TODO: include a test if the destination is a subvolume. It should be and this should be tested.
        if targetname is None:
            targetname = 'local'
    # Now we need to send the snapshot (incrementally, if possible), but only those
    # that did not have problems before
    for (sourceloc, sourcesnap, snapdir) in snapshots_to_backup:
        latest = os.path.join(snapdir, '.latest.' + targetname + '.' +
                              os.path.basename(sourceloc))
        real_latest = os.path.realpath(latest)
        if trial:
            print("trial: searching realpath of latest symlink %r for source %r" % (latest, sourceloc))
        else:
            print("searching realpath of latest symlink %r for source %r" % (real_latest, sourceloc))
        if os.path.exists(real_latest):
            print('sending incremental backup from', sourcesnap,
                'to', backuploc, 'using base', real_latest, file=sys.stderr)
            send_snapshot(sourcesnap, backuploc, real_latest, debug=args.debug, trial=trial,
                          remote_backup_command=remote_backup_command)
            if args.latest_only:
                print('removing old snapshot', real_latest, file=sys.stderr)
                delete_snapshot(real_latest, trial)
        else:
            print('initial snapshot successful; sending full backup from', sourcesnap,
                'to', backuploc, file=sys.stderr)
            send_snapshot(sourcesnap, backuploc, debug=args.debug, trial=trial,
                          remote_backup_command=remote_backup_command)
        if trial:
            print("trial: would change latest link %r to point to %r" % (latest, sourcesnap))
        else:
            print("changing latest link %r to point to %r" % (latest, sourcesnap))
            if os.path.islink(latest):
                print("unlinking %r" % latest)
                os.unlink(latest)
            elif os.path.exists(latest):
                problems.append('confusion:', latest, "should be a symlink but is not")
                continue
            # Make .latest point to this backup, using local symlink in same directory,
            # i.e avoid path name in link destination.
            print('New snapshot', sourcesnap, 'created (this is now latest', latest, ').',
                  file=sys.stderr)
            os.symlink(os.path.basename(sourcesnap), latest)

    if len(problems) > 0:
        print("Problem summary:\n * " + "\n * ".join(problems) +
              "\nBackup might be incomplete.", file=sys.stderr)
        sys.exit(1) # indicate failure
    print('Backup complete.', file=sys.stderr)

    # cleanup backups > NUM_BACKUPS in backup target
    if (NUM_BACKUPS > 0):
        for (sourceloc, sourcesnap, snapdir) in snapshots_to_backup:
            delete_old_backups(backuploc, sourceloc, NUM_BACKUPS, trial)

    sys.exit(0)     # indicate success
