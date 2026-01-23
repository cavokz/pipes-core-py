#!/usr/bin/env python3

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

import os
import sys
from contextlib import ExitStack
from pathlib import Path

import typer
from typing_extensions import Annotated, List, Optional

from .util import fatal, get_node, setup_logging

main = typer.Typer(pretty_exceptions_enable=False)


def parse_runtime_arguments(arguments):
    for arg in arguments or []:
        name, *value = arg.split("=")
        yield name, "=".join(value)


def configure_runtime_args_env(runtime, args_env, values, pipes, logger):
    import ast

    from .util import set_node, walk_args_env

    for name, type in sorted(walk_args_env(pipes, args_env)):
        value = values.get(name) or None
        if value is not None:
            if type is not str:
                try:
                    value = ast.literal_eval(value)
                except Exception:
                    pass
            logger.debug(f"  {name}: {value}")
            set_node(runtime.setdefault(args_env, {}), name, value)


def configure_runtime_arguments(runtime, arguments, pipes, logger):
    logger.debug("reading command line arguments")
    arguments = dict(parse_runtime_arguments(arguments))
    configure_runtime_args_env(runtime, "arguments", arguments, pipes, logger)


def configure_runtime_environment(runtime, environment, pipes, logger):
    logger.debug("reading environment variables")
    configure_runtime_args_env(runtime, "environment", environment, pipes, logger)


def configure_runtime(state, config_file, arguments, environment, logger):
    if config_file is sys.stdin:
        base_dir = Path.cwd()
    else:
        base_dir = Path(config_file.name).parent

    base_dir = str(base_dir.absolute())
    if base_dir not in sys.path:
        logger.debug(f"adding '{base_dir}' to the search path")
        sys.path.append(base_dir)

    state.setdefault("runtime", {}).update(
        {
            "base-dir": base_dir,
            "in-memory-state": True,
        }
    )

    pipes = load_pipes(state, logger)
    configure_runtime_arguments(state["runtime"], arguments, pipes, logger)
    configure_runtime_environment(state["runtime"], environment, pipes, logger)
    return pipes


def load_pipes(state, logger):
    from importlib import import_module

    from . import Pipe, get_pipes

    pipes = get_pipes(state)

    if pipes:
        name, config = pipes[0]
        if name == "elastic.pipes":
            for path in get_node(config, "search-path", None) or []:
                path = str(Path(state["runtime"]["base-dir"]) / path)
                if path not in sys.path:
                    logger.debug(f"adding '{path}' to the search path")
                    sys.path.append(path)

    for name, config in pipes:
        if name in Pipe.__pipes__:
            continue
        logger.debug(f"loading pipe '{name}'...")
        try:
            import_module(name)
        except ModuleNotFoundError as e:
            fatal(f"cannot load pipe '{name}': cannot find module: '{e.name}'")
        if name not in Pipe.__pipes__:
            fatal(f"module does not define a pipe: {name}")

    return [(Pipe.find(name), config) for name, config in pipes]


def explain_everything(pipes, logger):
    from rich import print
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    from .util import walk_config_nodes

    arguments = {}
    for pipe, root_name, node, help, notes, type, name, arg_name in walk_config_nodes(pipes, "runtime.arguments."):
        logger.debug(f"pipe: {pipe.name}, root: {root_name}, help: {help}, param name: {name}, arg name: {arg_name}")
        arguments.setdefault(arg_name, []).append((pipe, node, help, type, name))

    environment = {}
    for pipe, root_name, node, help, notes, type, name, arg_name in walk_config_nodes(pipes, "runtime.environment."):
        logger.debug(f"pipe: {pipe.name}, root: {root_name}, help: {help}, param name: {name}, arg name: {arg_name}")
        environment.setdefault(arg_name, []).append((pipe, node, help, type, name))

    def _render_panel(title, entries):
        table = Table(show_header=False, box=None, expand=False)
        for arg in sorted(entries):
            subtable = Table(show_header=False, box=None, expand=False)
            for pipe, node, help, type, name in entries[arg]:
                subtable.add_row(
                    Text("*", style="bold green"),
                    f"{help} in [i]{pipe.func.__doc__}[/i]",
                )
            table.add_row(
                Text(arg, style="bold cyan"),
                subtable,
            )
        if not entries:
            table.add_row("[i]none[/i]")
        return Panel(table, title=title, title_align="left", border_style="dim")

    print(_render_panel("Arguments", arguments))
    print(_render_panel("Environment", environment))


@main.command()
def run(
    config_file: typer.FileText,
    dry_run: Annotated[bool, typer.Option()] = False,
    explain: Annotated[bool, typer.Option(help="Describe what the script does.")] = False,
    log_level: Annotated[str, typer.Option(callback=setup_logging("INFO"))] = None,
    arguments: Annotated[Optional[List[str]], typer.Option("--argument", "-a", help="Pass an argument to the Pipes runtime.")] = None,
):
    """
    Run pipes
    """
    import logging

    from .errors import Error
    from .util import deserialize_yaml, warn_interactive

    logger = logging.getLogger("elastic.pipes.core")

    try:
        warn_interactive(config_file)
        state = deserialize_yaml(config_file) or {}
    except FileNotFoundError as e:
        fatal(f"{e.strerror}: '{e.filename}'")

    if not state:
        fatal("invalid configuration, it's empty")

    pipes = configure_runtime(state, config_file, arguments, os.environ, logger)

    if explain:
        explain_everything(pipes, logger)
        sys.exit(0)

    for pipe, config in pipes:
        try:
            pipe.check_config(config)
        except Error as e:
            pipe.logger.critical(e)
            sys.exit(1)

    with ExitStack() as stack:
        for pipe, config in pipes:
            try:
                pipe.run(config, state, dry_run, logger, stack)
            except Error as e:
                pipe.logger.critical(e)
                sys.exit(1)


@main.command()
def new_pipe(
    pipe_file: Path,
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
):
    """
    Create a new pipe module
    """

    pipe_file = pipe_file.with_suffix(".py")

    try:
        with pipe_file.open("w" if force else "x") as f:
            f.write(
                """#!/usr/bin/env python3

from logging import Logger

from elastic.pipes.core import Pipe
from typing_extensions import Annotated


@Pipe(default={}, notes="Use this example pipe as starting point for yours.")
def main(
    log: Logger,
    name: Annotated[
        str,
        Pipe.State("name"),
        Pipe.Help("whom to say hello"),
    ] = "world",
    age: Annotated[
        int,
        Pipe.State("age"),
        Pipe.Help("age of whom to say hello"),
    ] = -1,
    dry_run: bool = False,
):
    \"\"\"Say hello to someone.\"\"\"

    log.info(f"Hello, {{name}}!")


if __name__ == "__main__":
    main()
"""
            )
    except FileExistsError as e:
        fatal(f"{e.strerror}: '{e.filename}'")

    # make it executable
    mode = pipe_file.stat().st_mode
    pipe_file.chmod(mode | 0o111)


@main.command()
def version():
    """
    Print the version
    """
    from ..core import __version__

    print(__version__)
