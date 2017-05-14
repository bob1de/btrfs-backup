import os
import subprocess
import logging

import util


def require_snapdir(method):
    """Decorator that ensures snapdir is set on the object the called
       method belongs to."""
    def wrapped(self, *args, **kwargs):
        if self.snapdir is None:
            raise ValueError("snapdir hasn't been set")
        return method(self, *args, **kwargs)
    return wrapped

def require_no_snapdir(method):
    """Decorator that ensures snapdir is not set on the object the called
       method belongs to."""
    def wrapped(self, *args, **kwargs):
        if self.snapdir is not None:
            raise ValueError("snapdir has been set which is not allowed "
                             "for this kind of task")
        return method(self, *args, **kwargs)
    return wrapped


class Endpoint:
    def __init__(self, path="", snapprefix="", snapdir=None, btrfs_debug=False):
        self.path = path
        self.snapprefix = snapprefix
        self.snapdir = snapdir
        if snapprefix:
            self.lastname = "." + snapprefix + "_latest"
        else:
            self.lastname = ".latest"
        self.btrfs_debug = btrfs_debug
        self.btrfs_flags = []
        if self.btrfs_debug:
            self.btrfs_flags += ["-vv"]

    def __repr__(self):
        return self.path

    def prepare(self):
        pass

    def get_latest_snapshot(self):
        raise NotImplemented()

    def set_latest_snapshot(self, snapname):
        raise NotImplemented()

    def snapshot(self):
        raise NotImplemented()

    def send(self, *args, **kwargs):
        raise NotImplemented()

    def receive(self, *args, **kwargs):
        raise NotImplemented()

    def sync(self):
        logging.warning("Syncing disks is not (yet) supported for "
                        "{}".format(self))

    def subvolume_sync(self):
        logging.warning("Syncing subvolumes is not (yet) supported for "
                        "{}".format(self))

    @require_snapdir
    def list_snapshots(self):
        return self._list_snapshots(self.snapdir)

    @require_no_snapdir
    def list_backups(self):
        return self._list_snapshots(self.path)

    def delete_snapshot(self, location, convert_rw=False):
        logging.warning("Listing / deleting snapshots is not (yet) supported "
                        "for {}".format(self))

    @require_snapdir
    def delete_old_snapshots(self, keep_num, convert_rw=False):
        self._delete_old_snapshots(self.snapdir, keep_num,
                                   convert_rw=convert_rw)

    @require_no_snapdir
    def delete_old_backups(self, keep_num, convert_rw=False):
        self._delete_old_snapshots(self.path, keep_num, convert_rw=convert_rw)

    def _listdir(self, location):
        logging.warning("Listing / deleting snapshots is not (yet) supported "
                        "for {}".format(self))
        return []

    def _list_snapshots(self, location):
        snapnames = []
        for item in self._listdir(location):
            if item.startswith(self.snapprefix):
                time_str = item[len(self.snapprefix):]
                try:
                    util.str2date(time_str)
                except ValueError:
                    # no valid name for current prefix + time string
                    continue
                else:
                    snapnames.append(item)
        return snapnames

    def _delete_old_snapshots(self, location, keep_num, convert_rw=False):
        time_objs = []
        for item in self._list_snapshots(location):
            time_str = item[len(self.snapprefix):]
            try:
                time_objs.append(util.str2date(time_str))
            except ValueError:
                # no valid name for current prefix + time string
                continue

        # sort by date, then time;
        time_objs.sort()

        while time_objs and len(time_objs) > keep_num:
            # delete oldest snapshot
            to_remove = os.path.join(location, self.snapprefix +
                                     util.date2str(time_objs.pop(0)))
            self.delete_snapshot(to_remove, convert_rw=convert_rw)


class LocalEndpoint(Endpoint):
    def __init__(self, fstype_check=False, subvol_check=True, **kwargs):
        super(LocalEndpoint, self).__init__(**kwargs)
        self.path = os.path.abspath(self.path)
        if self.snapdir and not self.snapdir.startswith("/"):
            self.snapdir = os.path.join(self.path, self.snapdir)
        self.fstype_check = fstype_check
        self.subvol_check = subvol_check

    def prepare(self):
        # Ensure directories exist
        dirs = [self.path]
        if self.snapdir is not None:
            dirs.append(self.snapdir)
        for d in dirs:
            if not os.path.exists(d):
                try:
                    os.makedirs(d)
                except Exception as e:
                    logging.error("Error creating new location {}: {}".format(
                        d, e))
                    raise util.AbortError()
        if self.fstype_check and not util.is_btrfs(self.path):
            logging.error("{} does not seem to be on a btrfs "
                          "filesystem".format(self.path))
            raise util.AbortError()
        if self.subvol_check and not util.is_subvolume(self.path):
            logging.error("{} does not seem to be a btrfs "
                          "subvolume".format(self.path))
            raise util.AbortError()

    @require_snapdir
    def get_latest_snapshot(self):
        latest = os.path.join(self.snapdir, self.lastname)
        real_latest = os.path.realpath(latest)
        if os.path.exists(real_latest):
            return os.path.basename(real_latest)
        return None

    @require_snapdir
    def set_latest_snapshot(self, snapname):
        latest = os.path.join(self.snapdir, self.lastname)
        if os.path.islink(latest):
            os.unlink(latest)
        elif os.path.exists(latest):
            logging.error("Confusion: '{}' should be a symlink".format(latest))
        # Make .latest point to snapname - use relative symlink
        os.symlink(snapname, latest)
        logging.info("Latest snapshot is now: {}".format(snapname))

    @require_snapdir
    def snapshot(self, readonly=True):
        snapname = self.snapprefix + util.date2str()
        snaploc = os.path.join(self.snapdir, snapname)
        logging.info(util.log_heading("Snapshotting ..."))
        logging.info("  {} -> {}".format(self.path, snaploc))
        cmd = ['btrfs', 'subvolume', 'snapshot']
        if readonly:
            cmd += ['-r']
        cmd += [self.path, snaploc]
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError:
            logging.error("Error on command: {}".format(cmd))
            logging.error("Snapshot failed")
            raise util.AbortError()
        return snapname

    def send(self, snapname, parent=None):
        """Calls 'btrfs send' for the given snapshot and returns its
           Popen object."""
        cmd = ["btrfs", "send"] + self.btrfs_flags
        # from WARNING level onwards, pass --quiet
        loglevel = logging.getLogger().getEffectiveLevel()
        if loglevel >= logging.WARNING:
            cmd += ["--quiet"]
        if parent:
            cmd += ["-p", os.path.join(self.snapdir, parent)]
        cmd += [os.path.join(self.snapdir, snapname)]
        return subprocess.Popen(cmd, stdout=subprocess.PIPE)

    def receive(self, stdin):
        """Calls 'btrfs receive', setting the given pipe as its stdin.
           The receiving process's Popen object is returned."""
        cmd = ["btrfs", "receive"] + self.btrfs_flags + [self.path]
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        return subprocess.Popen(cmd, stdin=stdin, stdout=stdout)

    def sync(self):
        """Calls 'sync'."""
        logging.info(util.log_heading("Syncing disks ..."))
        cmd = ['sync']
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError:
            logging.error("Error on command: {}".format(cmd))

    def subvolume_sync(self):
        logging.info("Running 'btrfs subvolume sync' for {} "
                     "...".format(self.path))
        cmd = ["btrfs", "subvolume", "sync", self.path]
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError:
            logging.error("Error on command: {}".format(cmd))

    def delete_snapshot(self, location, convert_rw=False):
        logging.info("Removing snapshot: {}".format(location))
        if convert_rw:
            logging.info("  converting to read-write ...")
            cmd = ["btrfs", "property", "set", "-ts", location, "ro", "false"]
            try:
                subprocess.check_output(cmd)
            except subprocess.CalledProcessError:
                logging.error("Error on command: {}".format(cmd))
                return None
        logging.info("  deleting ...")
        cmd = ["btrfs", "subvolume", "delete", location]
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError:
            logging.error("Error on command: {}".format(cmd))

    def _listdir(self, location):
        return os.listdir(location)


class ShellEndpoint(Endpoint):
    def __init__(self, cmd, **kwargs):
        super(ShellEndpoint, self).__init__(**kwargs)
        self.cmd = cmd

    def __repr__(self):
        return "(Shell) " + self.cmd

    def receive(self, stdin):
        """Calls the given command, setting the given pipe as its stdin.
           The receiving process's Popen object is returned."""
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        return subprocess.Popen(self.cmd, stdin=stdin, stdout=stdout,
                                shell=True)


class SSHEndpoint(Endpoint):
    def __init__(self, hostname, port, username=None, ssh_opts=None, **kwargs):
        super(SSHEndpoint, self).__init__(**kwargs)
        self.username = username
        self.hostname = hostname
        self.port = port
        self.ssh_opts = ssh_opts or []

    def __repr__(self):
        return "(SSH) {}{}".format(
            self._get_connect_string(with_port=True), self.path)

    def _get_connect_string(self, with_port=False):
        s = self.hostname
        if self.username:
            s = "{}@{}".format(self.username, s)
        if with_port:
            s = "{}:{}".format(s, self.port)
        return s

    def receive(self, stdin):
        cmd = ["ssh"]
        for opt in self.ssh_opts:
            cmd += ["-o", opt]
        if self.port:
            cmd += ["-p", str(self.port)]
        connect_string = self._get_connect_string()
        cmd += [connect_string]
        cmd += ["btrfs", "receive"] + self.btrfs_flags + [self.path]
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        logging.debug("Executing subprocess: {}".format(cmd))
        return subprocess.Popen(cmd, stdin=stdin, stdout=stdout)