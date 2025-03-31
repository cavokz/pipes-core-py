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

import logging
import sys
from pathlib import Path

import typer
from typing_extensions import Annotated, List, Optional

from .util import fatal, get_node, set_node, warn_interactive

main = typer.Typer(pretty_exceptions_enable=False)


def setup_logging(log_level):
    import logging

    # a single handler to rule them all
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(name)s - %(message)s"))
    # root of all the elastic.pipes.* loggers
    logger = logging.getLogger("elastic.pipes")
    # all the pipes sync their handlers with this
    logger.addHandler(handler)

    # all the pipes sync their log level with this, unless configured differently
    if log_level is None:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(log_level.upper())
        logger.info("log level is overridden by the command line")
        logger.overridden = True


def sync_logger_config(logger, config):
    elastic_pipes_logger = logging.getLogger("elastic.pipes")
    if logger == elastic_pipes_logger:
        return
    for handler in reversed(logger.handlers):
        logger.removeHandler(handler)
    for handler in elastic_pipes_logger.handlers:
        logger.addHandler(handler)
    level = get_node(config, "logging.level", None)
    if level is None or getattr(elastic_pipes_logger, "overridden", False):
        logger.setLevel(elastic_pipes_logger.level)
    else:
        logger.setLevel(level.upper())


def parse_runtime_arguments(arguments):
    import ast

    args = {}
    for arg in arguments:
        name, *value = arg.split("=")
        if not value:
            set_node(args, name, None)
            continue
        value = value[0]
        if not value:
            set_node(args, name, None)
            continue
        try:
            value = ast.literal_eval(value)
        except ValueError:
            pass
        set_node(args, name, value)

    return args


@main.command()
def run(
    config_file: typer.FileText,
    dry_run: Annotated[bool, typer.Option()] = False,
    log_level: Annotated[str, typer.Option(callback=setup_logging)] = None,
    arguments: Annotated[Optional[List[str]], typer.Option("--argument", "-a")] = None,
):
    """
    Run pipes
    """
    from importlib import import_module
    from inspect import signature

    from . import Pipe, get_pipes
    from .errors import Error
    from .util import deserialize_yaml

    logger = logging.getLogger("elastic.pipes.core")

    try:
        warn_interactive(config_file)
        state = deserialize_yaml(config_file) or {}
    except FileNotFoundError as e:
        fatal(f"{e.strerror}: '{e.filename}'")

    if not state:
        fatal("invalid configuration, it's empty")

    if config_file.name == "<stdin>":
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

    if arguments:
        state["runtime"]["arguments"] = parse_runtime_arguments(arguments)

    pipes = get_pipes(state)

    if pipes:
        name, config = pipes[0]
        if name == "elastic.pipes":
            for path in get_node(config, "search-path", None) or []:
                path = str(Path(base_dir) / path)
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

    for name, config in pipes:
        pipe = Pipe.__pipes__[name]
        sync_logger_config(pipe.logger, config)
        if not dry_run:
            logger.debug(f"executing pipe '{name}'...")
        elif "dry_run" in signature(pipe.func).parameters:
            logger.debug(f"dry executing pipe '{name}'...")
        else:
            logger.debug(f"not executing pipe '{name}'...")

        try:
            pipe.run(config, state, dry_run, logger)
        except Error as e:
            fatal(e)


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
                f"""#!/usr/bin/env python3

from elastic.pipes.core import Pipe
from typing_extensions import Annotated


@Pipe("{pipe_file.stem}", default={{}})
def main(
    pipe: Pipe,
    dry_run: bool = False,
    name: Annotated[str, Pipe.State("name")] = "world",
):
    pipe.logger.info(f"Hello, {{name}}!")


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
