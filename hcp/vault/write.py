#!/usr/bin/env python3

import sys
from logging import Logger

import hvac
from elastic.pipes.core import Pipe
from typing_extensions import Annotated

from .common import Context


@Pipe("elastic.pipes.hcp.vault.write")
def main(
    log: Logger,
    ctx: Context,
    path: Annotated[
        str,
        Pipe.Config("path"),
        Pipe.Help("Vault path destination of the data"),
    ],
    vault: Annotated[
        dict,
        Pipe.State("vault", mutable=True),
        Pipe.Help("state node containing the source data"),
    ],
):
    """Write data to an HCP Vault instance."""

    log.info(f"writing to path '{path}'")

    vc = hvac.Client(url=ctx.url, token=ctx.token)

    try:
        if not vc.is_authenticated():
            log.error("Vault could not authenticate")
            sys.exit(1)
    except Exception as e:
        log.error(e)
        sys.exit(1)

    vc.write_data(path, data=vault)


if __name__ == "__main__":
    main()
