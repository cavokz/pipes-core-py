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
from pathlib import Path

import typer
from typing_extensions import Annotated

from ..core.util import fatal

main = typer.Typer(pretty_exceptions_enable=False)


def setup_logging():
    import logging

    # a single handler to rule them all
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(name)s - %(message)s"))
    # root of all the elastic.pipes.* loggers
    logger = logging.getLogger("elastic.pipes")
    # all the pipes sync their handlers with this
    logger.addHandler(handler)
    # all the pipes sync their log level with this, unless configured differently
    logger.setLevel(logging.INFO)


@main.command()
def run(
    config_file: typer.FileText,
    dry_run: Annotated[bool, typer.Option()] = False,
):
    """
    Run pipes
    """
    import yaml

    from ..core import Pipe
    from ..core.errors import Error

    setup_logging()

    try:
        state = yaml.safe_load(config_file) or {}
    except FileNotFoundError as e:
        fatal(f"{e.strerror}: '{e.filename}'")

    if config_file.name == "<stdin>":
        base_dir = Path.cwd()
    else:
        base_dir = Path(config_file.name).parent
    base_dir = str(base_dir.absolute())
    if base_dir not in sys.path:
        sys.path.append(base_dir)

    state.setdefault("runtime", {})["base-dir"] = base_dir

    try:
        Pipe.run(state, dry_run=dry_run)
    except Error as e:
        print(e, file=sys.stderr)
        sys.exit(1)


@main.command()
def new(
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


@Pipe("{pipe_file.stem}", default={{}})
def main(pipe, dry_run=False):
    pipe.logger.info("Hello, world!")


if __name__ == "__main__":
    main()
"""
            )
    except FileExistsError as e:
        fatal(f"{e.strerror}: '{e.filename}'")

    # make it executable
    mode = pipe_file.stat().st_mode
    pipe_file.chmod(mode | 0o111)
