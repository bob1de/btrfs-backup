import os
import time
import argparse


DATEFORMAT = '%Y%m%d-%H%M%S'


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
        format = DATEFORMAT
    return time.strftime(format, timestamp)

def str2date(timestring=None, format=None):
    if timestring is None:
        return time.localtime()
    if format is None:
        format = DATEFORMAT
    return time.strptime(timestring, format)

def is_btrfs(path):
    """Checks whether path is inside a btrfs file system"""
    path = os.path.normpath(os.path.abspath(path))
    best_match = ''
    best_match_fstype = ''
    for line in open("/proc/mounts"):
        try:
            mountpoint, fstype = line.split(" ")[1:3]
        except ValueError:
            continue
        if path.startswith(mountpoint) and len(mountpoint) > len(best_match):
            best_match = mountpoint
            best_match_fstype = fstype
    return best_match_fstype == "btrfs"

def is_subvolume(path):
    """Checks whether the given path is a btrfs subvolume."""
    if not is_btrfs(path):
        return False
    # subvolumes always have inode 256
    st = os.stat(path)
    return st.st_ino == 256
