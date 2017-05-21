import os
import subprocess
import tempfile
import logging

from .. import util
from .common import Endpoint


class SSHEndpoint(Endpoint):
    def __init__(self, hostname, port=None, username=None, ssh_opts=None,
                 **kwargs):
        super(SSHEndpoint, self).__init__(**kwargs)
        self.hostname = hostname
        self.port = port
        self.username = username
        self.ssh_opts = ssh_opts or []
        self.sshfs_opts = ["auto_unmount", "cache=no", "reconnect"]
        if self.source:
            self.source = os.path.normpath(self.source)
            if not self.path.startswith("/"):
                self.path = os.path.join(self.source, self.path)
        self.path = os.path.normpath(self.path)
        self.sshfs = False

    def __repr__(self):
        return "(SSH) {}{}".format(
            self._build_connect_string(with_port=True), self.path)

    def get_id(self):
        s = self.hostname
        if self.username:
            s = "{}@{}".format(self.username, s)
        if self.port:
            s = "{}:{}".format(s, self.port)
        return "ssh://{}{}".format(s, self.path)

    def prepare(self):
        # check whether ssh is available
        logging.debug("Checking for ssh ...")
        cmd = ["ssh"]
        try:
            util.exec_subprocess(cmd, method="call", stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
        except FileNotFoundError as e:
            logging.debug("  -> got exception: {}".format(e))
            logging.info("ssh command is not available")
            raise util.AbortError()
        else:
            logging.debug("  -> ssh is available")

        # sshfs is useful for listing directories and reading/writing locks
        tempdir = tempfile.mkdtemp()
        logging.debug("Created tempdir: {}".format(tempdir))
        mountpoint = os.path.join(tempdir, "mnt")
        os.makedirs(mountpoint)
        logging.debug("Created directory: {}".format(mountpoint))
        logging.debug("Mounting sshfs ...")

        cmd = ["sshfs"]
        if self.port:
            cmd += ["-p", str(self.port)]
        for opt in self.ssh_opts + self.sshfs_opts:
            cmd += ["-o", opt]
        cmd += ["{}:/".format(self._build_connect_string()), mountpoint]
        try:
            util.exec_subprocess(cmd, method="call", stdout=subprocess.DEVNULL)
        except FileNotFoundError as e:
            logging.debug("  -> got exception: {}".format(e))
            if self.source:
                # we need that for the locks
                logging.info("sshfs command is not available")
                raise util.AbortError()
        else:
            self.sshfs = mountpoint
            logging.debug("  -> sshfs is available")

    def _collapse_cmds(self, cmds, abort_on_failure=True):
        """Concatenates all given commands, ';' is inserted as separator."""

        collapsed = []
        for i, cmd in enumerate(cmds):
            if isinstance(cmd, (list, tuple)):
                collapsed.extend(cmd)
                if len(cmds) > i + 1:
                    collapsed.append("&&" if abort_on_failure else ";")

        return [collapsed]

    def _exec_cmd(self, orig_cmd, **kwargs):
        """Executes the command at the remote host."""

        cmd = ["ssh"]
        if self.port:
            cmd += ["-p", str(self.port)]
        for opt in self.ssh_opts:
            cmd += ["-o", opt]
        cmd += [self._build_connect_string()]
        cmd.extend(orig_cmd)

        return util.exec_subprocess(cmd, **kwargs)

    def _listdir(self, location):
        """Operates remotely via 'ls -1a'. '.' and '..' are excluded from
           the result."""

        if self.sshfs:
            items = os.listdir(self._path2sshfs(location))
        else:
            cmd = ["ls", "-1a", location]
            output = self._exec_cmd(cmd, universal_newlines=True)
            items = []
            for item in output.splitlines():
                # remove . and ..
                if item not in (".", ".."):
                    items.append(item)

        return items

    def _get_lock_file_path(self):
        return self._path2sshfs(super(SSHEndpoint, self)._get_lock_file_path())


    ########## Custom methods

    def _build_connect_string(self, with_port=False):
        s = self.hostname
        if self.username:
            s = "{}@{}".format(self.username, s)
        if with_port and self.port:
            s = "{}:{}".format(s, self.port)
        return s

    def _path2sshfs(self, path):
        """Joins the given ``path`` with the sshfs mountpoint."""
        if not self.sshfs:
            raise ValueError("sshfs not mounted")
        if path.startswith("/"):
            path = path[1:]
        return os.path.join(self.sshfs, path)
