import os
import subprocess
import logging

from .. import util


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
                 subvolume_sync=False, btrfs_debug=False, source=None,
                 fs_checks=True, **kwargs):
        self.path = path
        self.snapprefix = snapprefix
        self.convert_rw = convert_rw
        self.subvolume_sync = subvolume_sync
        self.btrfs_debug = btrfs_debug
        self.btrfs_flags = []
        if self.btrfs_debug:
            self.btrfs_flags += ["-vv"]
        self.source = source
        self.fs_checks = fs_checks
        self.lock_file_name = ".outstanding_transfers"
        self.__cached_snapshots = None

    def prepare(self):
        logging.info("Preparing endpoint {} ...".format(self))
        return self._prepare()

    @require_source
    def snapshot(self, readonly=True, sync=True):
        """Takes a snapshot and returns the created object."""

        snapshot = util.Snapshot(self.path, self.snapprefix, self)
        snapshot_path = snapshot.get_path()
        logging.info("{} -> {}".format(self.source, snapshot_path))

        cmds = []
        cmds.append(self._build_snapshot_cmd(self.source, snapshot_path,
                                             readonly=readonly))

        # sync disks
        if sync:
            cmds.append(self._build_sync_cmd())

        for cmd in self._collapse_cmds(cmds, abort_on_failure=True):
            self._exec_cmd(cmd)

        self.add_snapshot(snapshot)
        return snapshot

    @require_source
    def send(self, snapshot, parent=None, clones=None):
        """Calls 'btrfs send' for the given snapshot and returns its
           Popen object."""

        cmd = self._build_send_cmd(snapshot, parent=parent, clones=clones)
        return self._exec_cmd(cmd, method="Popen", stdout=subprocess.PIPE)

    def receive(self, stdin):
        """Calls 'btrfs receive', setting the given pipe as its stdin.
           The receiving process's Popen object is returned."""

        cmd = self._build_receive_cmd(self.path)
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        return self._exec_cmd(cmd, method="Popen", stdin=stdin, stdout=stdout)

    def list_snapshots(self, flush_cache=False):
        """Returns a list with all snapshots found at ``self.path``.
           If ``flush_cache`` is not set, cached results will be used
           if available."""

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
        if self.source:
            lock_dict = self._read_locks()
            for snapshot in snapshots:
                snap_entry = lock_dict.get(snapshot.get_name(), {})
                for lock_type, locks in snap_entry.items():
                    getattr(snapshot, lock_type).update(locks)

        # sort by date, then time;
        snapshots.sort()

        # populate cache
        self.__cached_snapshots = snapshots
        logging.debug("Populated snapshot cache of {} with {} "
                      "items.".format(self, len(snapshots)))

        return list(snapshots)

    @require_source
    def set_lock(self, snapshot, lock_id, lock_state, parent=False):
        """Adds/removes the given lock from ``snapshot`` and calls
           ``_write_locks`` with the updated locks."""
        if lock_state:
            if parent:
                snapshot.parent_locks.add(lock_id)
            else:
                snapshot.locks.add(lock_id)
        else:
            if parent:
                snapshot.parent_locks.discard(lock_id)
            else:
                snapshot.locks.discard(lock_id)
        lock_dict = {}
        for _snapshot in self.list_snapshots():
            snap_entry = {}
            if _snapshot.locks:
                snap_entry["locks"] = list(_snapshot.locks)
            if _snapshot.parent_locks:
                snap_entry["parent_locks"] = list(_snapshot.parent_locks)
            if snap_entry:
                lock_dict[_snapshot.get_name()] = snap_entry
        self._write_locks(lock_dict)
        logging.debug("Lock state for {} and lock_id {} changed to {} (parent "
                      "= {})".format(snapshot, lock_id, lock_state, parent))

    def add_snapshot(self, snapshot, rewrite=True):
        """Adds a snapshot to the cache. If ``rewrite`` is set, a new
           ``util.Snapshot`` object is created with the original ``prefix``
           and ``time_obj``. However, ``path`` and ``endpoint`` are set to
           belong to this endpoint. The original snapshot object is
           dropped in that case."""

        if self.__cached_snapshots is None:
            return None

        if rewrite:
            snapshot = util.Snapshot(self.path, snapshot.prefix, self,
                                     time_obj=snapshot.time_obj)

        self.__cached_snapshots.append(snapshot)
        self.__cached_snapshots.sort()

    def delete_snapshots(self, snapshots, **kwargs):
        """Deletes the given snapshots, passing all keyword arguments to
           ``_build_deletion_cmds``."""

        # only remove snapshots that have no lock remaining
        to_remove = []
        for snapshot in snapshots:
            if not snapshot.locks and not snapshot.parent_locks:
                to_remove.append(snapshot)

        logging.info("Removing {} snapshot(s) from "
                     "{}:".format(len(to_remove), self))
        for snapshot in snapshots:
            if snapshot in to_remove:
                logging.info("  {}".format(snapshot))
            else:
                logging.info("  {} - is locked, keeping it".format(snapshot))

        if to_remove:
            # finally delete them
            cmds = self._build_deletion_cmds(to_remove, **kwargs)
            cmds = self._collapse_cmds(cmds, abort_on_failure=True)
            for cmd in cmds:
                self._exec_cmd(cmd)

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


    # The following methods may be implemented by endpoints unless the
    # default behaviour is wanted.

    def __repr__(self):
        return self.path

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return "unknown://{}".format(self.path)

    def _prepare(self):
        """Is called after endpoint creation. Various endpoint-related
           checks may be implemented here."""
        pass

    def _build_snapshot_cmd(self, source, dest, readonly=True):
        """Should return a command which, when executed, creates a
           snapshot of ``source`` at ``dest``. If ``readonly`` is set,
           the snapshot should be read only."""
        cmd = ["btrfs", "subvolume", "snapshot"]
        if readonly:
            cmd += ["-r"]
        cmd += [source, dest]
        return cmd

    def _build_sync_cmd(self):
        """Should return the 'sync' command."""
        return ["sync"]

    def _build_send_cmd(self, snapshot, parent=None, clones=None):
        """Should return a command which, when executed, writes the send
           stream of given ``snapshot`` to stdout. ``parent`` and ``clones``
           may be used as well."""
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
        return cmd

    def _build_receive_cmd(self, dest):
        """Should return a command to receive a snapshot to ``dest``.
           The stream is piped into stdin when the command is running."""
        return ["btrfs", "receive"] + self.btrfs_flags + [dest]

    def _build_deletion_cmds(self, snapshots, convert_rw=None,
                             subvolume_sync=None):
        """Should return a list of commands that, when executed in order,
           delete the given ``snapshots``. ``convert_rw`` and
           ``subvolume_sync`` should be regarded as well."""

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

    def _collapse_cmds(self, cmds, abort_on_failure=True):
        """This might be re-implemented to group commands together whereever
           possible. The default implementation simply returns the given command
           list unchanged.
           If ``abort_on_failure`` is set, the implementation must assure that
           every collapsed command in the returned list aborts immediately
           after one of the original commands included in it fail. If it is
           unset, the opposite behaviour is expected (subsequent commands have
           be run even in case a previous one fails)."""

        return cmds

    def _exec_cmd(self, cmd, **kwargs):
        """Finally, the command should be executed via
           ``util.exec_subprocess``, which should get all given keyword
           arguments. This could be re-implemented to execute via SSH,
           for instance."""
        return util.exec_subprocess(cmd, **kwargs)

    def _listdir(self, location):
        """Should return all items present at the given ``location``."""
        return os.listdir(location)

    @require_source
    def _get_lock_file_path(self):
        """Is used by the default ``_read/write_locks`` methods and should
           return the file in which the locks are stored."""
        return os.path.join(self.path, self.lock_file_name)

    @require_source
    def _read_locks(self):
        """Should read the locks and return a dict like
           ``util.read_locks`` returns it."""
        path = self._get_lock_file_path()
        try:
            if not os.path.isfile(path):
                return {}
            with open(path, "r") as f:
                return util.read_locks(f.read())
        except (OSError, ValueError) as e:
            logging.error("Error on reading lock file {}: "
                          "{}".format(path, e))
            raise util.AbortError()

    @require_source
    def _write_locks(self, lock_dict):
        """Should write the locks given as ``lock_dict`` like
           ``util.read_locks`` returns it."""
        path = self._get_lock_file_path()
        try:
            logging.debug("Writing lock file: {}".format(path))
            with open(path, "w") as f:
                f.write(util.write_locks(lock_dict))
        except OSError as e:
            logging.error("Error on writing lock file {}: "
                          "{}".format(path, e))
            raise util.AbortError()
