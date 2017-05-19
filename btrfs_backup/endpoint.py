import os
import subprocess
import logging

from . import util


def require_source(method):
    """Decorator that ensures source is set on the object the called
       method belongs to."""
    def wrapped(self, *args, **kwargs):
        if self.source is None:
            raise ValueError("source hasn't been set")
        return method(self, *args, **kwargs)
    return wrapped


class Endpoint:
    def __init__(self, path=None, snapprefix="", convert_rw=False,
                 subvolume_sync=False, btrfs_debug=False, source=None):
        self.path = path
        self.snapprefix = snapprefix
        self.btrfs_debug = btrfs_debug
        self.btrfs_flags = []
        if self.btrfs_debug:
            self.btrfs_flags += ["-vv"]
        self.convert_rw = convert_rw
        self.subvolume_sync = subvolume_sync
        self.source = source
        self.__cached_snapshots = None

    def __repr__(self):
        return self.path

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return "unknown://{}".format(self.path)

    def prepare(self):
        pass

    @require_source
    def snapshot(self, **kwargs):
        return self._snapshot(**kwargs)

    def _snapshot(self, readonly=True, sync=True):
        raise NotImplemented()

    def send(self, snapshot, parent=None, clones=None):
        raise NotImplemented()

    def receive(self, *args, **kwargs):
        raise NotImplemented()

    def list_snapshots(self, flush_cache=False):
        if self.__cached_snapshots is not None and not flush_cache:
            logging.debug("Returning {} cached snapshots for "
                          "{}.".format(len(self.__cached_snapshots), self))
            return list(self.__cached_snapshots)
        logging.debug("Building snapshot cache of {} ...".format(self))
        snapshots = []
        listdir = self._listdir(self.path)
        for item in listdir:
            if item.startswith(self.snapprefix):
                time_str = item[len(self.snapprefix):]
                try:
                    time_obj = util.str2date(time_str)
                except ValueError:
                    # no valid name for current prefix + time string
                    continue
                else:
                    snapshot = util.Snapshot(self.path, self.snapprefix, self,
                                             time_obj=time_obj)
                    snapshots.append(snapshot)

        # apply locks
        lock_dict = self._read_locks()
        for snapshot in snapshots:
            snapshot.locks.update(lock_dict.get(snapshot.get_name(), []))

        # sort by date, then time;
        snapshots.sort()
        # populate cache
        self.__cached_snapshots = snapshots
        logging.debug("Populated snapshot cache of {} with {} "
                      "items.".format(self, len(snapshots)))
        return list(snapshots)

    def _read_locks(self):
        """Should read the locks and return a dict like
           ``util.read_locks`` returns it."""
        return {}

    def set_lock(self, snapshot, lock_id, lock_state):
        """Should add/remove the given lock from snapshot and write locks
           out to permanent storage."""
        raise NotImplemented()

    def add_snapshot(self, snapshot, rewrite=True):
        if self.__cached_snapshots is None:
            return None
        if rewrite:
            snapshot = util.Snapshot(self.path, snapshot.prefix, self,
                                     time_obj=snapshot.time_obj)
        self.__cached_snapshots.append(snapshot)
        self.__cached_snapshots.sort()

    def delete_snapshots(self, snapshots, ignore_locks=False, **kwargs):
        # only remove snapshots that have no lock remaining
        to_remove = []
        for snapshot in snapshots:
            if not snapshot.locks or ignore_locks:
                to_remove.append(snapshot)
                # remove existing locks, if any
                for lock in set(snapshot.locks):
                    self.set_lock(snapshot, lock, False)
        logging.info("Removing {} snapshot(s) from "
                     "{}:".format(len(to_remove), self))
        for snapshot in snapshots:
            if snapshot in to_remove:
                logging.info("  {}".format(snapshot))
            else:
                logging.info("  {} - is locked, keeping it".format(snapshot))
        if to_remove:
            self._delete_snapshots(to_remove, **kwargs)
            if self.__cached_snapshots is not None:
                for snapshot in to_remove:
                    try:
                        self.__cached_snapshots.remove(snapshot)
                    except ValueError:
                        pass

    def delete_snapshot(self, snapshot, **kwargs):
        self.delete_snapshots([snapshot], **kwargs)

    def delete_old_snapshots(self, keep_num, **kwargs):
        snapshots = self.list_snapshots()

        if len(snapshots) > keep_num:
            # delete oldest snapshots
            to_remove = snapshots[:-keep_num]
            self.delete_snapshots(to_remove, **kwargs)

    def _build_deletion_cmds(self, snapshots, convert_rw=None,
                             subvolume_sync=None):
        if convert_rw is None:
            convert_rw = self.convert_rw
        if subvolume_sync is None:
            subvolume_sync = self.subvolume_sync
        cmds = []
        if convert_rw:
            for snapshot in snapshots:
                cmds.append(["btrfs", "property", "set", "-ts",
                             snapshot.get_path(), "ro", "false"])
        cmd = ["btrfs", "subvolume", "delete"]
        cmd.extend([snapshot.get_path() for snapshot in snapshots])
        cmds.append(cmd)
        if subvolume_sync:
            cmds.append(["btrfs", "subvolume", "sync", self.path])
        return cmds

    def _listdir(self, location):
        logging.warning("Listing / deleting snapshots is not (yet) supported "
                        "for {}".format(self))
        return []

    def _delete_snapshots(self, snapshots, **kwargs):
        logging.warning("Listing / deleting snapshots is not (yet) supported "
                        "for {}".format(self))


class LocalEndpoint(Endpoint):
    def __init__(self, fs_checks=True, **kwargs):
        super(LocalEndpoint, self).__init__(**kwargs)
        if self.source is not None:
            self.source = os.path.abspath(self.source)
            if not self.path.startswith("/"):
                self.path = os.path.join(self.source, self.path)
        else:
            self.path = os.path.abspath(self.path)
        self.fs_checks = fs_checks
        lock_name = ".outstanding_transfers"
        self.lock_path = os.path.join(self.path, lock_name)

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return self.path

    def prepare(self):
        # Ensure directories exist
        dirs = []
        if self.source is not None:
            dirs.append(self.source)
        dirs.append(self.path)
        for d in dirs:
            if os.path.exists(d):
                logging.debug("Directory exists: {}".format(d))
            else:
                logging.info("Creating directory: {}".format(d))
                try:
                    os.makedirs(d)
                except Exception as e:
                    logging.error("Error creating new location {}: "
                                  "{}".format(d, e))
                    raise util.AbortError()
        if self.source is not None and self.fs_checks and \
           not util.is_subvolume(self.source):
            logging.error("{} does not seem to be a btrfs "
                          "subvolume".format(self.source))
            raise util.AbortError()
        if self.fs_checks and not util.is_btrfs(self.path):
            logging.error("{} does not seem to be on a btrfs "
                          "filesystem".format(self.path))
            raise util.AbortError()

    @require_source
    def _snapshot(self, readonly=True, sync=True):
        snapshot = util.Snapshot(self.path, self.snapprefix, self)
        snapshot_path = snapshot.get_path()
        logging.info("{} -> {}".format(self.source, snapshot_path))
        cmd = ["btrfs", "subvolume", "snapshot"]
        if readonly:
            cmd += ["-r"]
        cmd += [self.source, snapshot_path]
        logging.debug("Executing: {}".format(cmd))
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError:
            logging.error("Error on command: {}".format(cmd))
            logging.error("Snapshot failed")
            raise util.AbortError()
        # sync disks
        if sync:
            logging.info("Syncing disks ...")
            cmd = ["sync"]
            logging.debug("Executing: {}".format(cmd))
            try:
                subprocess.check_output(cmd)
            except subprocess.CalledProcessError:
                logging.error("Error on command: {}".format(cmd))
        return snapshot

    def send(self, snapshot, parent=None, clones=None):
        """Calls 'btrfs send' for the given snapshot and returns its
           Popen object."""
        cmd = ["btrfs", "send"] + self.btrfs_flags
        # from WARNING level onwards, pass --quiet
        loglevel = logging.getLogger().getEffectiveLevel()
        if loglevel >= logging.WARNING:
            cmd += ["--quiet"]
        if parent:
            cmd += ["-p", parent.get_path()]
        if clones:
            for clone in clones:
                cmd += ["-c", clone.get_path()]
        cmd += [snapshot.get_path()]
        logging.debug("Executing: {}".format(cmd))
        return subprocess.Popen(cmd, stdout=subprocess.PIPE)

    def receive(self, stdin):
        """Calls 'btrfs receive', setting the given pipe as its stdin.
           The receiving process's Popen object is returned."""
        cmd = ["btrfs", "receive"] + self.btrfs_flags + [self.path]
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        logging.debug("Executing: {}".format(cmd))
        return subprocess.Popen(cmd, stdin=stdin, stdout=stdout)

    def _read_locks(self):
        try:
            if not os.path.isfile(self.lock_path):
                return {}
            with open(self.lock_path, "r") as f:
                return util.read_locks(f.read())
        except (OSError, ValueError) as e:
            logging.error("Error on reading lock file {}: "
                          "{}".format(self.lock_path, e))
            raise util.AbortError()

    def set_lock(self, snapshot, lock_id, lock_state):
        try:
            if lock_state:
                snapshot.locks.add(lock_id)
            else:
                snapshot.locks.discard(lock_id)
            lock_dict = {}
            for _snapshot in self.list_snapshots():
                if _snapshot.locks:
                    lock_dict[_snapshot.get_name()] = list(_snapshot.locks)
            logging.debug("Writing lock file: {}".format(self.lock_path))
            with open(self.lock_path, "w") as f:
                f.write(util.write_locks(lock_dict))
        except OSError as e:
            logging.error("Error on writing lock file {}: "
                          "{}".format(self.lock_path, e))
            raise util.AbortError()
        logging.debug("Lock state for {} and lock_id {} changed to "
                      "{}".format(snapshot, lock_id, lock_state))

    def _delete_snapshots(self, snapshots, **kwargs):
        cmds = self._build_deletion_cmds(snapshots, **kwargs)
        for cmd in cmds:
            logging.debug("Executing: {}".format(cmd))
            try:
                subprocess.check_output(cmd)
            except subprocess.CalledProcessError as e:
                logging.debug("  -> got exception: {}".format(e))
                logging.error("Couldn't delete snapshots at {}".format(self))
                raise util.AbortError()

    def _listdir(self, location):
        return os.listdir(location)


class ShellEndpoint(Endpoint):
    def __init__(self, cmd, **kwargs):
        super(ShellEndpoint, self).__init__(**kwargs)
        self.cmd = cmd

    def __repr__(self):
        return "(Shell) " + self.cmd

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return "shell://{}".format(self.cmd)

    def receive(self, stdin):
        """Calls the given command, setting the given pipe as its stdin.
           The receiving process's Popen object is returned."""
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        logging.debug("Executing: {}".format(self.cmd))
        return subprocess.Popen(self.cmd, stdin=stdin, stdout=stdout,
                                shell=True)

    def add_snapshot(self, *args, **kwargs):
        """Adding not supported. This is just a stub."""
        pass


class SSHEndpoint(Endpoint):
    def __init__(self, hostname, port=None, username=None, ssh_opts=None,
                 **kwargs):
        super(SSHEndpoint, self).__init__(**kwargs)
        self.hostname = hostname
        self.port = port
        self.username = username
        self.ssh_opts = ssh_opts or []

    def __repr__(self):
        return "(SSH) {}{}".format(
            self._build_connect_string(with_port=True), self.path)

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        s = self.hostname
        if self.username:
            s = "{}@{}".format(self.username, s)
        if self.port:
            s = "{}:{}".format(s, self.port)
        return "ssh://{}{}".format(s, self.path)

    def _build_connect_string(self, with_port=False):
        s = self.hostname
        if self.username:
            s = "{}@{}".format(self.username, s)
        if with_port and self.port:
            s = "{}:{}".format(s, self.port)
        return s

    def _build_ssh_cmd(self, cmds=None, multi=False):
        def append_cmd(append_to, to_append):
            if isinstance(to_append, (list, tuple)):
                append_to.extend(to_append)
            elif isinstance(to_append, str):
                append_to.append(to_append)
            else:
                return False
            return True

        cmd = ["ssh"]
        if self.port:
            cmd += ["-p", str(self.port)]
        for opt in self.ssh_opts:
            cmd += ["-o", opt]
        cmd += [self._build_connect_string()]
        if multi:
            for i, _cmd in enumerate(cmds):
                if append_cmd(cmd, _cmd) and i+1 < len(cmds):
                    cmd.append(";")
        else:
            append_cmd(cmd, cmds)
        return cmd

    def prepare(self):
        # check whether ssh is available
        logging.debug("Checking for ssh ...")
        cmd = ["ssh"]
        logging.debug("Executing: {}".format(cmd))
        try:
            subprocess.call(cmd, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
        except FileNotFoundError as e:
            logging.debug("  -> got exception: {}".format(e))
            logging.info("ssh command is not available")
            raise util.AbortError()
        else:
            logging.debug("  -> ssh is available")

    def receive(self, stdin):
        cmd = ["btrfs", "receive"] + self.btrfs_flags + [self.path]
        cmd = self._build_ssh_cmd(cmd)
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        logging.debug("Executing: {}".format(cmd))
        return subprocess.Popen(cmd, stdin=stdin, stdout=stdout)

    def _listdir(self, location):
        cmd = ["ls", "-1a", location]
        cmd = self._build_ssh_cmd(cmd)
        logging.debug("Executing: {}".format(cmd))
        try:
            output = subprocess.check_output(cmd, universal_newlines=True)
        except subprocess.CalledProcessError as e:
            logging.debug("  -> got exception: {}".format(e))
            logging.error("Couldn't list {} at {}".format(location, self))
            raise util.AbortError()
        items = []
        for item in output.splitlines():
            # remove . and ..
            if item not in (".", ".."):
                items.append(item)
        return items

    def _delete_snapshots(self, snapshots, **kwargs):
        cmds = self._build_deletion_cmds(snapshots, **kwargs)
        cmd = self._build_ssh_cmd(cmds, multi=True)
        logging.debug("Executing: {}".format(cmd))
        try:
            subprocess.check_output(cmd, universal_newlines=True)
        except subprocess.CalledProcessError as e:
            logging.debug("  -> got exception: {}".format(e))
            logging.error("Couldn't delete snapshots at {}".format(self))
            raise util.AbortError()
