import subprocess
import logging

from .common import Endpoint


class ShellEndpoint(Endpoint):
    def __init__(self, cmd, **kwargs):
        super(ShellEndpoint, self).__init__(**kwargs)
        self.cmd = cmd

    def __repr__(self):
        return "(Shell) " + self.cmd

    def get_id(self):
        """Return an id string to identify this endpoint over multiple runs."""
        return "shell://{}".format(self.cmd)

    def receive(self, stdin):
        """Calls the given command, setting the given pipe as its stdin.
           The receiving process's Popen object is returned."""
        # from WARNING level onwards, hide stdout
        loglevel = logging.getLogger().getEffectiveLevel()
        stdout = subprocess.DEVNULL if loglevel >= logging.WARNING else None
        return util.exec_subprocess(self.cmd, method="Popen", stdin=stdin,
                                    stdout=stdout, shell=True)
