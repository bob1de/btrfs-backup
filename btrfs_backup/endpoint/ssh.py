import os
import subprocess
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

    def __repr__(self):
        return "(SSH) {}{}".format(
            self._build_connect_string(with_port=True), self.path)

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        s = self.hostname
        if self.username:
            s = "{}@{}".format(self.username, s)
        if self.port:
            s = "{}:{}".format(s, self.port)
        return "ssh://{}{}".format(s, self.path)

    def _build_connect_string(self, with_port=False):
        s = self.hostname
        if self.username:
            s = "{}@{}".format(self.username, s)
        if with_port and self.port:
            s = "{}:{}".format(s, self.port)
        return s

    def _build_ssh_cmd(self, cmds=None, multi=False):
        def append_cmd(append_to, to_append):
            if isinstance(to_append, (list, tuple)):
                append_to.extend(to_append)
            elif isinstance(to_append, str):
                append_to.append(to_append)
            else:
                return False
            return True

        cmd = ["ssh"]
        if self.port:
            cmd += ["-p", str(self.port)]
        for opt in self.ssh_opts:
            cmd += ["-o", opt]
        cmd += [self._build_connect_string()]
        if multi:
            for i, _cmd in enumerate(cmds):
                if append_cmd(cmd, _cmd) and i+1 < len(cmds):
                    cmd.append(";")
        else:
            append_cmd(cmd, cmds)
        return cmd

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

    def receive(self, stdin):
        cmd = ["btrfs", "receive"] + self.btrfs_flags + [self.path]
        cmd = self._build_ssh_cmd(cmd)
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        return util.exec_subprocess(cmd, method="Popen", stdin=stdin,
                                    stdout=stdout)

    def _listdir(self, location):
        cmd = ["ls", "-1a", location]
        cmd = self._build_ssh_cmd(cmd)
        output = util.exec_subprocess(cmd, universal_newlines=True)
        items = []
        for item in output.splitlines():
            # remove . and ..
            if item not in (".", ".."):
                items.append(item)
        return items

    def _delete_snapshots(self, snapshots, **kwargs):
        cmds = self._build_deletion_cmds(snapshots, **kwargs)
        cmd = self._build_ssh_cmd(cmds, multi=True)
        util.exec_subprocess(cmd, universal_newlines=True)
