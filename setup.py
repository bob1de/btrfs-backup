#!/usr/bin/env python3

import os
from setuptools import setup

from btrfs_backup import __version__


def read_file(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name = "btrfs_backup",
    version = __version__,
    description = "Intelligent, feature-rich backups for btrfs",
    long_description = read_file("README.rst"),
    url = "https://github.com/efficiosoft/btrfs-backup",
    author = "Robert Schindler",
    author_email = "r.schindler@efficiosoft.com",
    license = "MIT",
    packages = ["btrfs_backup", "btrfs_backup.endpoint"],
    zip_safe = False,
    entry_points = {
       "console_scripts": [
            "btrfs-backup = btrfs_backup.__main__:main",
        ],
    },
)
