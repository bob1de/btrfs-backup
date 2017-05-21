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

    def _collapse_cmds(self, cmds):
        """Concatenates all given commands, ';' is inserted as separator."""

        collapsed = []
        for i, cmd in enumerate(cmds):
            if isinstance(cmd, (list, tuple)):
                collapsed.extend(cmd)
                if len(cmds) > i + 1:
                    collapsed.append(";")

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
        cmd = ["ls", "-1a", location]
        output = self._exec_cmd(cmd, universal_newlines=True)
        items = []
        for item in output.splitlines():
            # remove . and ..
            if item not in (".", ".."):
                items.append(item)
        return items


    ########## Custom methods

    def _build_connect_string(self, with_port=False):
        s = self.hostname
        if self.username:
            s = "{}@{}".format(self.username, s)
        if with_port and self.port:
            s = "{}:{}".format(s, self.port)
        return s
