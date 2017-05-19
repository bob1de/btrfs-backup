#!/usr/bin/env python3

from setuptools import setup

from btrfs_backup import __version__

setup(
    name = "btrfs_backup",
    description = "Intelligent, feature-rich backups for btrfs",
    version = __version__,
    url = "https://github.com/efficiosoft/btrfs-backup",
    author = "Robert Schindler",
    author_email = "r.schindler@efficiosoft.com",
    license = "MIT",
    packages = ["btrfs_backup"],
    zip_safe = False,
    entry_points = {
       "console_scripts": [
            "btrfs-backup = btrfs_backup.__main__:main",
        ],
    },
)
