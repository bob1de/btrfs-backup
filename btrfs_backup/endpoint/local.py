import os
import subprocess
import logging

from .. import util
from .common import Endpoint


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

    def _snapshot(self, readonly=True, sync=True):
        snapshot = util.Snapshot(self.path, self.snapprefix, self)
        snapshot_path = snapshot.get_path()
        logging.info("{} -> {}".format(self.source, snapshot_path))
        cmd = ["btrfs", "subvolume", "snapshot"]
        if readonly:
            cmd += ["-r"]
        cmd += [self.source, snapshot_path]
        util.exec_subprocess(cmd)
        # sync disks
        if sync:
            logging.info("Syncing disks ...")
            cmd = ["sync"]
            try:
                util.exec_subprocess(cmd)
            except util.AbortError:
                pass
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
        return util.exec_subprocess(cmd, method="Popen", stdout=subprocess.PIPE)

    def receive(self, stdin):
        """Calls 'btrfs receive', setting the given pipe as its stdin.
           The receiving process's Popen object is returned."""
        cmd = ["btrfs", "receive"] + self.btrfs_flags + [self.path]
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        return util.exec_subprocess(cmd, method="Popen", stdin=stdin,
                                    stdout=stdout)

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
            util.exec_subprocess(cmd)

    def _listdir(self, location):
        return os.listdir(location)
