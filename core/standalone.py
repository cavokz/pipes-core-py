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
from contextlib import ExitStack

from . import get_pipes
from .errors import Error
from .util import (
    deserialize_yaml,
    fatal,
    serialize_yaml,
    setup_logging,
    warn_interactive,
)


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


def help_message(pipe):
    from functools import partial

    from rich import print
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    from . import Pipe
    from .util import walk_contexts, walk_params

    pipe_doc = pipe.func.__doc__
    if not pipe_doc:
        pipe_doc = "[i]This pipe has no description.[/i]"

    config_entries = []
    state_entries = []

    for node, type, help, notes, default, empty in walk_params(pipe):
        help = help or ""
        if isinstance(node, Pipe.Config):
            if notes is None:
                notes = "" if default is empty else f"default: {repr(default)}"
            config_entries.append([node.node, type.__name__, help, notes])
        if isinstance(node, Pipe.State):
            if node.node and node.node.startswith("runtime."):
                continue
            if node.node is not None:
                if notes is None:
                    notes = "" if default is empty else f"default: {repr(default)}"
                state_entries.append([node.node, type.__name__, help, notes])
            elif indirect := node.get_indirect_node_name():
                if notes is None:
                    notes = f"default: {repr(node.node)}"
                config_entries.append([indirect, type.__name__, help, notes])

    notes = []
    if pipe.notes:
        notes += pipe.notes if isinstance(pipe.notes, list) else [str(pipe.notes)]
    for ctx in walk_contexts(pipe):
        ctx_notes = getattr(ctx, "notes", None) or []
        notes += ctx_notes if isinstance(ctx_notes, list) else [str(ctx_notes)]
    if pipe.closing_notes:
        notes += pipe.closing_notes if isinstance(pipe.closing_notes, list) else [str(pipe.closing_notes)]

    def _render_panel(title, entries):
        table = Table(show_header=False, box=None, expand=False)
        for entry in sorted(entries):
            table.add_row(
                Text(entry[0], style="bold cyan"),
                Text(entry[1], style="bold yellow"),
                entry[2],
                Text(entry[3], style="dim"),
            )
        if not entries:
            table.add_row("[i]none[/i]")
        return Panel(table, title=title, title_align="left", border_style="dim")

    def _render_notes(notes):
        table = Table(show_header=False, box=None, expand=False)
        for note in notes:
            table.add_row(
                Text("*", style="bold green"),
                note,
            )
        return Panel(table, title="Notes", title_align="left", border_style="dim")

    # print everything on standard error
    print = partial(print, file=sys.stderr)

    print(pipe_doc)
    print()
    print(_render_panel("Configuration parameters", config_entries))
    print(_render_panel("State nodes", state_entries))
    if notes:
        print(_render_notes(notes))
    print()
    print("Use the [bold green]-p[/bold green] option to execute in UNIX pipe mode.")


def run(pipe):
    import typer
    from typing_extensions import Annotated

    def _main(
        dry_run: Annotated[bool, typer.Option()] = False,
        log_level: Annotated[str, typer.Option(callback=setup_logging("DEBUG"))] = None,
        pipe_mode: Annotated[
            bool,
            typer.Option(
                "--pipe-mode",
                "-p",
                rich_help_panel="UNIX pipe mode",
                help="Read state from standard input and write state to standard output. This is the default mode when executed in a UNIX pipe.",
            ),
        ] = False,
        describe: Annotated[
            bool,
            typer.Option(
                "--describe",
                help="Show detailed info about this pipe.",
            ),
        ] = False,
    ):
        logger = logging.getLogger("elastic.pipes.core")

        if describe or sys.stdin.isatty() and not pipe_mode:
            help_message(pipe)
            sys.exit(0)

        try:
            state = receive_state_from_unix_pipe(pipe.logger, pipe.default)
            pipes = get_pipes(state)
        except Error as e:
            fatal(e)

        configs = [c for n, c in pipes if n == pipe.name]
        config = configs[0] if configs else {}

        try:
            pipe.check_config(config, logger)
        except Error as e:
            # use core logger here because the pipe logger is not properly configured yet
            logger.critical(f"{pipe.name}: {e}")
            sys.exit(1)

        with ExitStack() as stack:
            try:
                pipe.run(config, state, dry_run, logger, stack)
            except Error as e:
                pipe.logger.critical(e)
                sys.exit(1)
        send_state_to_unix_pipe(pipe.logger, state)

    typer.run(_main)
