# Copyright 2025 Elasticsearch B.V.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Core definitions for stand alone pipes invocation."""

import logging
import sys

from ..core import get_pipes
from ..core.errors import Error
from ..core.util import deserialize_yaml, fatal, serialize_yaml, warn_interactive


def receive_state_from_unix_pipe(logger, default):
    logger.debug("awaiting state from standard input")
    warn_interactive(sys.stdin)
    state = deserialize_yaml(sys.stdin)

    if state:
        logger.debug("got state")
    elif default is sys.exit:
        logger.debug("no state, exiting")
        sys.exit(1)
    else:
        logger.debug("using default state")
        state = default

    return state


def send_state_to_unix_pipe(logger, state):
    logger.debug("relaying state to standard output")
    serialize_yaml(sys.stdout, state)


def run(pipe):
    import typer
    from typing_extensions import Annotated

    def _main(
        dry_run: Annotated[bool, typer.Option()] = False,
        log_level: Annotated[str, typer.Option()] = None,
        interactive: Annotated[bool, typer.Option("--interactive", "-i", help="Allow entering the state via terminal.")] = False,
    ):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(name)s - %(message)s"))
        pipe.logger.addHandler(handler)

        if log_level is None:
            pipe.logger.setLevel(logging.DEBUG)
        else:
            pipe.logger.setLevel(log_level.upper())
            pipe.logger.info("log level is overridden by the command line")
            pipe.logger.overridden = True

        if sys.stdin.isatty() and not interactive:
            fatal("This is an Elastic Pipes component, use the `-i` option to execute it interactively.")

        try:
            pipe.state = receive_state_from_unix_pipe(pipe.logger, pipe.default)
            pipes = get_pipes(pipe.state)
        except Error as e:
            fatal(e)

        pipe.__config__ = {}
        for name, config in pipes:
            if pipe.name == name:
                pipe.__config__ = config
                break

        ret = pipe.func(pipe, dry_run)
        send_state_to_unix_pipe(pipe.logger, pipe.state)
        return ret

    typer.run(_main)
