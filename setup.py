#!/usr/bin/env python3

from setuptools import setup

setup(
    name = "btrfs_backup",
    description = "Incremental backups for btrfs",
    version = "0.2.1",
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
