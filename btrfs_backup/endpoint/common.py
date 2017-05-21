import os
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

    def _write_locks(self, lock_dict):
        """Should write the locks given as ``lock_dict`` like
           ``util.read_locks`` returns it."""
        raise NotImplemented()

    def set_lock(self, snapshot, lock_id, lock_state):
        """Adds/removes the given lock from ``snapshot`` and calls
           ``_write_locks`` with the updated locks."""
        if lock_state:
            snapshot.locks.add(lock_id)
        else:
            snapshot.locks.discard(lock_id)
        lock_dict = {}
        for _snapshot in self.list_snapshots():
            if _snapshot.locks:
                lock_dict[_snapshot.get_name()] = list(_snapshot.locks)
        self._write_locks(lock_dict)
        logging.debug("Lock state for {} and lock_id {} changed to "
                      "{}".format(snapshot, lock_id, lock_state))

    def add_snapshot(self, snapshot, rewrite=True):
        if self.__cached_snapshots is None:
            return None
        if rewrite:
            snapshot = util.Snapshot(self.path, snapshot.prefix, self,
                                     time_obj=snapshot.time_obj)
        self.__cached_snapshots.append(snapshot)
        self.__cached_snapshots.sort()

    def delete_snapshots(self, snapshots, **kwargs):
        # only remove snapshots that have no lock remaining
        to_remove = []
        for snapshot in snapshots:
            if not snapshot.locks:
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
