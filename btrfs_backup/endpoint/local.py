import os
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

    def _write_locks(self, lock_dict):
        try:
            logging.debug("Writing lock file: {}".format(self.lock_path))
            with open(self.lock_path, "w") as f:
                f.write(util.write_locks(lock_dict))
        except OSError as e:
            logging.error("Error on writing lock file {}: "
                          "{}".format(self.lock_path, e))
            raise util.AbortError()

    def _listdir(self, location):
        return os.listdir(location)
