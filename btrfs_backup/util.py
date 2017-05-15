import os
import time
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


class AbortError(Exception):
    pass


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
        return time.localtime()
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
