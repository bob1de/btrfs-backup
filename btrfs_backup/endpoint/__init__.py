import os
import urllib.parse

from .local import LocalEndpoint
from .ssh import SSHEndpoint
from .shell import ShellEndpoint


def choose_endpoint(spec, common_kwargs=None, source=False,
                    excluded_types=None):
    """Chooses a suitable endpoint based on the specification given.
       If ``common_kwargs`` is given, it should be a dictionary with
       keyword arguments that all endpoint types should be initialized
       with.
       If ``source`` is set, this is considered as a source endpoint,
       meaning that parsed path is passed as ``source`` parameter and not
       as ``path`` at endpoint initialization. The value for ``path``
       should be present in ``common_kwargs`` in this case.
       The endpoint classes specified in ``excluded_types`` are excluded
       from the consideration.
       It will return a instance of the proper ``Endpoint`` sub-class.
       If no endpoint can be determined for the given specification,
       a ``ValueError`` is raised."""

    kwargs = {}
    if common_kwargs:
        kwargs.update(common_kwargs)
    if not excluded_types:
        excluded_types = []

    # parse destination string
    if ShellEndpoint not in excluded_types and spec.startswith("shell://"):
        c = ShellEndpoint
        kwargs["cmd"] = spec[8:]
        kwargs["source"] = True
    elif SSHEndpoint not in excluded_types and spec.startswith("ssh://"):
        c = SSHEndpoint
        parsed = urllib.parse.urlparse(spec)
        if not parsed.hostname:
            raise ValueError("No hostname for SSh specified.")
        try:
            kwargs["port"] = parsed.port
        except ValueError:
            # invalid literal for int ...
            kwargs["port"] = None
        path = parsed.path.strip() or "/"
        if parsed.query:
            path += "?" + parsed.query
        path = os.path.normpath(path)
        if source:
            kwargs["source"] = path
        else:
            kwargs["path"] = path
        kwargs["username"] = parsed.username
        kwargs["hostname"] = parsed.hostname
    elif LocalEndpoint not in excluded_types:
        c = LocalEndpoint
        if source:
            kwargs["source"] = spec
        else:
            kwargs["path"] = spec
    else:
        raise ValueError("No endpoint could be generated for this "
                         "specification: {}".format(spec))

    return c(**kwargs)
