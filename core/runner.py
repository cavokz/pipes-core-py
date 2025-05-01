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

import sys
from contextlib import ExitStack
from pathlib import Path

import typer
from typing_extensions import Annotated, List, Optional

from .util import fatal, get_node, set_node, setup_logging, warn_interactive

main = typer.Typer(pretty_exceptions_enable=False)


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
        except Exception:
            pass
        set_node(args, name, value)

    return args


@main.command()
def run(
    config_file: typer.FileText,
    dry_run: Annotated[bool, typer.Option()] = False,
    log_level: Annotated[str, typer.Option(callback=setup_logging("INFO"))] = None,
    arguments: Annotated[Optional[List[str]], typer.Option("--argument", "-a", help="Pass an argument to the Pipes runtime.")] = None,
):
    """
    Run pipes
    """
    import logging
    from importlib import import_module

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
        state["runtime"].setdefault("arguments", {}).update(parse_runtime_arguments(arguments))

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

    with ExitStack() as stack:
        for name, config in pipes:
            pipe = Pipe.find(name)
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
                f"""#!/usr/bin/env python3

from logging import Logger

from elastic.pipes.core import Pipe
from typing_extensions import Annotated


@Pipe("{pipe_file.stem}", default={{}}, notes="Use this example pipe as starting point for yours.")
def main(
    log: Logger,
    name: Annotated[str, Pipe.State("name"), Pipe.Help("to whom say hello")] = "world",
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
