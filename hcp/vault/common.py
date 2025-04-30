import os
import sys

from elastic.pipes.core import Pipe
from typing_extensions import Annotated


class Context(Pipe.Context):
    url: Annotated[
        str,
        Pipe.Config("url"),
        Pipe.Help("URL of the Vault instance"),
        Pipe.Notes("default: from environment VAULT_ADDR"),
    ] = None
    token: Annotated[
        str,
        Pipe.Config("token"),
        Pipe.Help("Vault API authentication token"),
        Pipe.Notes("default: from environment VAULT_TOKEN"),
    ] = None

    def __init__(self):
        if not self.url:
            if url := os.environ.get("VAULT_ADDR", None):
                self.logger.debug("read URL from environment 'VAULT_ADDR'")
                self.url = url
        if not self.token:
            if token := os.environ.get("VAULT_TOKEN", None):
                self.logger.debug("read token from environment 'VAULT_TOKEN'")
                self.token = token

        if not self.url:
            self.logger.error("Vault URL is not defined")
            sys.exit(1)
        if not self.token:
            self.logger.error("Vault token is not defined")
            sys.exit(1)
