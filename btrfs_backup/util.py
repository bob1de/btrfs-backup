import functools
import os
import time
import json
import argparse
import logging


DATE_FORMAT = "%Y%m%d-%H%M%S"
MOUNTS_FILE = "/proc/mounts"


class ArgparseSmartFormatter(argparse.HelpFormatter):
    def _split_lines(self, text, width):
        if text.startswith("N|"):
            _lines = text[2:].splitlines()
        else:
            _lines = [text]
        lines = []
        for line in _lines:
            # this is the RawTextHelpFormatter._split_lines
            lines.extend(argparse.HelpFormatter._split_lines(self, line, width))
        return lines


def read_locks(s):
    """Reads locks from lock file content given as string.
       Returns ``{'snapname': ['lock', ...], ...}``.
       If format is invalid, ``ValueError`` is raised."""
    s = s.strip()
    lock_dict = {}
    if not s:
        return lock_dict
    try:
        content = json.loads(s)
        assert isinstance(content, dict)
        for snapname, locks in content.items():
            assert isinstance(snapname, str)
            assert isinstance(locks, list)
            if not locks:
                # ignore empty lock lists
                continue
            lock_dict[snapname] = []
            for lock in locks:
                assert isinstance(lock, str)
                if lock not in lock_dict[snapname]:
                    # don't add multiple identical locks
                    lock_dict[snapname].append(lock)
    except (AssertionError, json.JSONDecodeError) as e:
        logging.error("Lock file couldn't be parsed: {}".format(e))
        raise ValueError("invalid lock file format")
    return lock_dict

def write_locks(lock_dict):
    """Converts ``lock_dict`` back to the string readable by ``read_locks``."""
    return json.dumps(lock_dict, indent=4)


class AbortError(Exception):
    pass

class SnapshotTransferError(AbortError):
    pass


@functools.total_ordering
class Snapshot:
    """Represents a snapshot with comparison by prefix and time_obj."""
    def __init__(self, location, prefix, endpoint, time_obj=None):
        self.location = location
        self.prefix = prefix
        self.endpoint = endpoint
        if time_obj is None:
            time_obj = str2date()
        self.time_obj = time_obj
        self.locks = set()

    def __eq__(self, other):
        return self.prefix == other.prefix and self.time_obj == other.time_obj

    def __lt__(self, other):
        if self.prefix != other.prefix:
            raise NotImplemented("prefixes dont match: "
                                 "{} vs {}".format(self.prefix, other.prefix))
        return self.time_obj < other.time_obj

    def __repr__(self):
        return self.get_name()

    def get_name(self):
        return self.prefix + date2str(self.time_obj)

    def get_path(self):
        return os.path.join(self.location, self.get_name())

    def find_parent(self, present_snapshots):
        """Returns object from ``present_snapshot`` most suitable for being
           used as a parent for transferring this one or ``None``,
           if none found."""
        if self in present_snapshots:
            # snapshot already transferred
            return None
        for present_snapshot in reversed(present_snapshots):
            if present_snapshot < self:
                return present_snapshot
        # no snapshot older than snapshot is present ...
        if present_snapshots:
            # ... hence we choose the oldest one present as parent
            return present_snapshots[0]


def log_heading(caption):
    return "{:-<50}".format("--[ {} ]".format(caption))


def date2str(timestamp=None, format=None):
    if timestamp is None:
        timestamp = time.localtime()
    if format is None:
        format = DATE_FORMAT
    return time.strftime(format, timestamp)

def str2date(timestring=None, format=None):
    if timestring is None:
        # we don't simply return time.localtime() because this would have
        # a higher precision than the result converted from string
        timestring = date2str()
    if format is None:
        format = DATE_FORMAT
    return time.strptime(timestring, format)

def is_btrfs(path):
    """Checks whether path is inside a btrfs file system"""
    path = os.path.normpath(os.path.abspath(path))
    logging.debug("Checking for btrfs filesystem: {}".format(path))
    best_match = ""
    best_match_fstype = ""
    logging.debug("  Reading mounts file: {}".format(MOUNTS_FILE))
    for line in open(MOUNTS_FILE):
        try:
            mountpoint, fstype = line.split(" ")[1:3]
        except ValueError as e:
            logging.debug("  Couldn't split line, skipping: {}".format(line))
            continue
        if path.startswith(mountpoint) and len(mountpoint) > len(best_match):
            best_match = mountpoint
            best_match_fstype = fstype
            logging.debug("  New best_match with fstype {}: "
                          "{}".format(best_match_fstype, best_match))
    result = best_match_fstype == "btrfs"
    logging.debug("  -> best_match_fstype is {}, result is "
                  "{}".format(best_match_fstype, result))
    return result

def is_subvolume(path):
    """Checks whether the given path is a btrfs subvolume."""
    if not is_btrfs(path):
        return False
    logging.debug("Checking for btrfs subvolume: {}".format(path))
    # subvolumes always have inode 256
    st = os.stat(path)
    result = st.st_ino == 256
    logging.debug("  -> Inode is {}, result is {}".format(st.st_ino, result))
    return result
