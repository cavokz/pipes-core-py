#!/usr/bin/env python3

from logging import Logger

import hvac
from elastic.pipes.core import Pipe
from elastic.pipes.core.util import fatal
from typing_extensions import Annotated

from .common import update_url_token_from_env


@Pipe("elastic.pipes.hcp.vault.write")
def main(
    log: Logger,
    path: Annotated[str, Pipe.Config("path")],
    vault: Annotated[dict, Pipe.State("vault")],
    url: Annotated[str, Pipe.Config("url")] = None,
    token: Annotated[str, Pipe.Config("token")] = None,
):
    log.info(f"path: {path}")
    url, token = update_url_token_from_env(url, token, log)

    vc = hvac.Client(url=url, token=token)
    if not vc.is_authenticated():
        fatal("vault could not authenticate")

    vc.write_data(path, data=vault)


if __name__ == "__main__":
    main()
