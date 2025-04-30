#!/usr/bin/env python3

import sys
from logging import Logger

import hvac
from elastic.pipes.core import Pipe
from typing_extensions import Annotated

from .common import Context


@Pipe("elastic.pipes.hcp.vault.read")
def main(
    log: Logger,
    ctx: Context,
    path: Annotated[
        str,
        Pipe.Config("path"),
        Pipe.Help("Vault path containing the source data"),
    ],
    vault: Annotated[
        dict,
        Pipe.State("vault", mutable=True),
        Pipe.Help("state node destination of the data"),
    ],
):
    """Read data from an HCP Vault instance."""

    log.info(f"reading from path '{path}'")

    vc = hvac.Client(url=ctx.url, token=ctx.token)

    try:
        if not vc.is_authenticated():
            log.error("Vault could not authenticate")
            sys.exit(1)
    except Exception as e:
        log.error(e)
        sys.exit(1)

    res = vc.read(path)
    if res is None:
        log.error(f"could not read path: '{path}'")
        sys.exit(1)

    vault.clear()
    vault.update(res["data"])


if __name__ == "__main__":
    main()
