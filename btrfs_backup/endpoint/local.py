import os
import logging

from .. import util
from .common import Endpoint


class LocalEndpoint(Endpoint):
    def __init__(self, **kwargs):
        super(LocalEndpoint, self).__init__(**kwargs)
        if self.source:
            self.source = os.path.normpath(os.path.abspath(self.source))
            if not self.path.startswith("/"):
                self.path = os.path.join(self.source, self.path)
        else:
            self.path = os.path.abspath(self.path)
        self.path = os.path.normpath(self.path)

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return self.path

    def _prepare(self):
        # create directories, if needed
        dirs = []
        if self.source is not None:
            dirs.append(self.source)
        dirs.append(self.path)
        for d in dirs:
            if not os.path.isdir(d):
                logging.info("Creating directory: {}".format(d))
                try:
                    os.makedirs(d)
                except OSError as e:
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
