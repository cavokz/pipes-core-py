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

"""Utility functions for node access, serialization, and introspection.

Provides functions for navigating nested dictionaries using dot-separated
paths, serializing/deserializing state in multiple formats, and walking
pipe parameter definitions.
"""

import os
import re
import sys
from collections.abc import Mapping

from typing_extensions import Any, NoDefault, get_args, get_type_hints

from .errors import ConfigError, Error

if sys.version_info >= (3, 12):
    from itertools import batched
else:
    from itertools import islice

    def batched(iterable, chunk_size):
        """Batch iterable into chunks of specified size.

        Args:
            iterable: Input iterable to batch.
            chunk_size: Size of each batch.

        Yields:
            Lists of up to chunk_size elements.
        """
        iterator = iter(iterable)
        while chunk := list(islice(iterator, chunk_size)):
            yield chunk


# Pattern to match valid paths: segments separated by dots
# Segment can be either '[^']+' or "[^"]+" (quoted) or [^.'"]+ (unquoted, excluding dots and quotes)
_valid_path_pattern = re.compile(r"""^('[^']+'|"[^"]+"|[^.'"]+)(\.('[^']+'|"[^"]+"|[^.'"]+))*$""")

# Extract parts: either single-quoted (group 1), double-quoted (group 2), or unquoted segments (group 3)
_extract_path_pattern = re.compile(r"""'([^']+)'|"([^"]+)"|([^.'"]+)""")


def split_path(path):
    """Split dot-separated path into list of keys.

    Args:
        path: Dot-separated string like "elasticsearch.url" or None.

    Returns:
        Tuple of path components, or empty tuple if path is None.

    Raises:
        Error: If path is not a string or contains empty components.
    """
    if path is None:
        return ()
    if not isinstance(path, str):
        raise Error(f"invalid path: type is '{type(path).__name__}' (expected 'str')")
    if not _valid_path_pattern.match(path):
        raise Error(f"invalid path: {path}")
    matches = _extract_path_pattern.findall(path)
    return [m[0] if m[0] else (m[1] if m[1] else m[2]) for m in matches]


def quote_path_segment(segment):
    """Quote a literal path segment if it requires quoting.

    A segment must be quoted if it contains a dot (otherwise interpreted as
    a path separator) or a quote character (otherwise invalid in an
    unquoted segment). Segments needing none of these are returned
    unchanged.

    Args:
        segment: Literal (already unquoted) path segment value.

    Returns:
        The segment as-is, or wrapped in quotes if needed.

    Raises:
        Error: If the segment contains both quote characters, since neither
            quoting style can then represent it unambiguously.
    """
    if not any(c in segment for c in ".'\""):
        return segment
    if "'" not in segment:
        return f"'{segment}'"
    if '"' not in segment:
        return f'"{segment}"'
    raise Error(f"path segment cannot be quoted, contains both quote characters: {segment!r}")


def has_node(dict, path):
    """Check if node exists at path in dictionary.

    Args:
        dict: Dictionary to search.
        path: Dot-separated path string.

    Returns:
        True if path exists and has a value, False otherwise.
    """
    keys = split_path(path)
    for key in keys:
        if not isinstance(dict, Mapping):
            return False
        try:
            dict = dict[key]
        except KeyError:
            return False
    return dict is not None


def get_node(dict, path, default=NoDefault, *, default_action=None, shell_expand=False):
    """Retrieve value at path in nested dictionary.

    Args:
        dict: Dictionary to search.
        path: Dot-separated path string, or None for whole dict.
        default: Default value if path not found. If NoDefault, raises KeyError.
        default_action: Callable returning default value. Overrides default parameter.
        shell_expand: If True, pass value through shell expansion.

    Returns:
        Value at path, or default if not found.

    Raises:
        KeyError: If path not found and no default provided.
        Error: If path traverses through non-mapping value.
        ShellExpansionError: If shell_expand fails.
    """
    if default_action is None:

        def default_action():
            if default is NoDefault:
                raise KeyError(path)
            return default

    keys = split_path(path)
    for i, key in enumerate(keys):
        if dict is None:
            return default_action()
        if not isinstance(dict, Mapping):
            raise Error(f"not an object: {'.'.join(keys[:i])} (type is {type(dict).__name__})")
        try:
            dict = dict[key]
        except KeyError:
            return default_action()
    if dict is None:
        return default_action()
    if shell_expand:
        from .shelllib import shell_expand

        dict = shell_expand(dict)
    return dict


def set_node(dict, path, value):
    """Set value at path in nested dictionary.

    Creates intermediate dictionaries as needed. If path is None,
    replaces entire dict contents with value.

    Args:
        dict: Dictionary to modify.
        path: Dot-separated path string, or None.
        value: Value to set.

    Raises:
        Error: If path traverses through non-mapping value, or if setting
            root with non-mapping value.
    """
    keys = split_path(path)
    for i, key in enumerate(keys[:-1]):
        if not isinstance(dict, Mapping):
            raise Error(f"not an object: {'.'.join(keys[:i])} (type is {type(dict).__name__})")
        dict = dict.setdefault(key, {})
    if not isinstance(dict, Mapping):
        raise Error(f"not an object: {'.'.join(keys[:-1]) or 'None'} (type is {type(dict).__name__})")
    if keys:
        dict[keys[-1]] = value
        return
    if not isinstance(value, Mapping):
        raise Error(f"not an object: value type is {type(value).__name__}")
    dict.clear()
    dict.update(value)


def serialize_yaml(file, state):
    """Serialize state to YAML format.

    Args:
        file: File object to write to.
        state: Data structure to serialize.
    """
    import yaml

    try:
        # use the LibYAML C library bindings if available
        from yaml import CDumper as Dumper
    except ImportError:
        from yaml import Dumper

    yaml.dump(state, file, Dumper=Dumper)


def deserialize_yaml(file, *, streaming=False):
    """Deserialize YAML format to Python objects.

    Args:
        file: File object to read from.
        streaming: If True, raises ConfigError (YAML doesn't support streaming).

    Returns:
        Deserialized data structure.

    Raises:
        ConfigError: If streaming=True.
    """
    import yaml

    try:
        # use the LibYAML C library bindings if available
        from yaml import CLoader as Loader
    except ImportError:
        from yaml import Loader

    if streaming:
        raise ConfigError("cannot stream yaml (try ndjson)")

    return yaml.load(file, Loader=Loader)


def serialize_json(file, state):
    """Serialize state to JSON format.

    Args:
        file: File object to write to.
        state: Data structure to serialize.
    """
    import json

    file.write(json.dumps(state) + "\n")


def deserialize_json(file, *, streaming=False):
    """Deserialize JSON format to Python objects.

    Args:
        file: File object to read from.
        streaming: If True, raises ConfigError (JSON doesn't support streaming).

    Returns:
        Deserialized data structure.

    Raises:
        ConfigError: If streaming=True.
    """
    import json

    if streaming:
        raise ConfigError("cannot stream json (try ndjson)")

    return json.load(file)


def serialize_ndjson(file, state):
    """Serialize state to NDJSON format (newline-delimited JSON).

    Args:
        file: File object to write to.
        state: Sequence of objects to serialize, one per line.
    """
    import json

    for elem in state:
        file.write(json.dumps(elem) + "\n")


def deserialize_ndjson(file, *, streaming=False):
    """Deserialize NDJSON format to Python objects.

    Args:
        file: File object to read from.
        streaming: If True, returns generator instead of list.

    Returns:
        List of deserialized objects, or generator if streaming=True.
    """
    import json

    if streaming:
        return (json.loads(line) for line in file)

    return [json.loads(line) for line in file]


def serialize(file, state, *, format):
    """Serialize state to file in specified format.

    Args:
        file: File object to write to.
        state: Data structure to serialize.
        format: Format string: 'yaml', 'yml', 'json', or 'ndjson'.

    Raises:
        ConfigError: If format is unsupported.
    """
    if format in ("yaml", "yml"):
        serialize_yaml(file, state)
    elif format == "json":
        serialize_json(file, state)
    elif format == "ndjson":
        serialize_ndjson(file, state)
    else:
        raise ConfigError(f"unsupported format: {format}")


def deserialize(file, *, format, streaming=False):
    """Deserialize file to Python objects in specified format.

    Args:
        file: File object to read from.
        format: Format string: 'yaml', 'yml', 'json', or 'ndjson'.
        streaming: If True, return generator for ndjson. Raises error for other formats.

    Returns:
        Deserialized data structure, or generator if streaming ndjson.

    Raises:
        ConfigError: If format is unsupported or streaming requested for non-ndjson.
    """
    if format in ("yaml", "yml"):
        state = deserialize_yaml(file, streaming=streaming)
    elif format == "json":
        state = deserialize_json(file, streaming=streaming)
    elif format == "ndjson":
        state = deserialize_ndjson(file, streaming=streaming)
    else:
        raise ConfigError(f"unsupported format: {format}")
    return state


def fatal(msg):
    """Exit process with error message.

    In json mode, logs msg as a single ECS line and exits with status 1,
    since sys.exit(msg) would otherwise print msg as plain text (sys.excepthook
    is not invoked for the SystemExit that sys.exit(msg) raises).

    Args:
        msg: Error message to display.
    """
    import logging

    if sys.excepthook is _log_uncaught_exception:
        logging.getLogger("elastic.pipes.core").critical(msg)
        sys.exit(1)
    sys.exit(msg)


def warn_interactive(f):
    """Print instructions if reading from terminal interactively.

    Args:
        f: File object to check for tty.
    """
    if f.isatty():
        if os.name == "nt":
            print("Press CTRL-Z and ENTER to end", file=sys.stderr)
        else:
            print("Press CTRL-D one time (or two, if you entered any input) to end", file=sys.stderr)


def is_mutable(value):
    """Check if value is mutable (unhashable).

    Args:
        value: Value to test.

    Returns:
        True if value cannot be used as dictionary key (mutable).
    """
    d = {}
    try:
        d[value] = None
    except TypeError:
        return True
    return False


def _build_log_formatter(fmt):
    """Build a logging.Formatter for the given format name.

    Args:
        fmt: Format name, "text" or "json" (case-insensitive).

    Returns:
        A configured logging.Formatter instance. "json" produces
        ECS-compliant JSON lines via ecs_logging.StdlibFormatter.

    Raises:
        ConfigError: If fmt is not a recognized format name.
    """
    import logging

    name = fmt.lower()
    if name == "text":
        return logging.Formatter("%(name)s - %(message)s")
    if name == "json":
        import ecs_logging

        return ecs_logging.StdlibFormatter()
    raise ConfigError(f"invalid logging format: '{fmt}'")


def _ensure_log_handler():
    """Create and attach the shared elastic.pipes StreamHandler, once.

    Click/Typer process CLI options in the order they were given, not in
    declaration order, so --log-level and --log-format may run in either
    order: this makes handler creation idempotent regardless.

    Returns:
        The 'elastic.pipes' logger, with a handler attached.
    """
    import logging

    logger = logging.getLogger("elastic.pipes")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_build_log_formatter("text"))
        logger.addHandler(handler)
    return logger


_previous_excepthook = None


def _log_uncaught_exception(exc_type, exc_value, exc_tb):
    """Log an escaping exception as a single message instead of a raw traceback.

    Installed as sys.excepthook while the log format is "json", so that an
    exception escaping all pipe processing still produces a single
    ECS-compliant JSON log line, rather than a plain traceback that would
    break JSON-per-line output.

    Args:
        exc_type: Exception class.
        exc_value: Exception instance.
        exc_tb: Traceback object.
    """
    import logging

    logging.getLogger("elastic.pipes").critical("unhandled exception", exc_info=(exc_type, exc_value, exc_tb))


def set_logging_format(fmt):
    """Set the log line format for the elastic.pipes logger hierarchy.

    Switches the formatter on the shared handler(s) of the 'elastic.pipes'
    root logger. Every pipe logger references these same handler objects
    (see setup_logging), so the new format applies to all of them, for any
    log line emitted from this point forward.

    When fmt is "json", also installs a top-level exception handler (see
    _log_uncaught_exception) so that an escaping exception is captured as a
    single log message instead of a raw traceback. Switching back to "text"
    restores the exception handler that was in place before.

    Args:
        fmt: Format name, "text" or "json" (case-insensitive).

    Raises:
        ConfigError: If fmt is not a recognized format name.
    """
    global _previous_excepthook

    formatter = _build_log_formatter(fmt)
    logger = _ensure_log_handler()
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    if fmt.lower() == "json":
        if sys.excepthook is not _log_uncaught_exception:
            _previous_excepthook = sys.excepthook
            sys.excepthook = _log_uncaught_exception
    elif sys.excepthook is _log_uncaught_exception:
        sys.excepthook = _previous_excepthook or sys.__excepthook__
        _previous_excepthook = None


def log_interrupt(func):
    """Decorate a Typer command to log a KeyboardInterrupt, only in json mode.

    Typer converts an escaping KeyboardInterrupt into a silent exit(130)
    before sys.excepthook ever sees it: this runs first, from inside the
    command, then re-raises so that exit(130) still happens.

    Args:
        func: Command function to decorate.

    Returns:
        Wrapped function.
    """
    import logging
    from functools import wraps

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            if sys.excepthook is _log_uncaught_exception:
                logging.getLogger("elastic.pipes.core").warning("interrupted")
            raise

    return wrapper


def setup_logging(default_level="NOTSET"):
    """Create logging configuration function.

    Args:
        default_level: Default log level if not overridden.

    Returns:
        Function that takes a level argument and configures the
        elastic.pipes logger hierarchy.
    """

    def _handler(level):
        logger = _ensure_log_handler()

        # all the pipes sync their log level with this, unless configured differently
        if level is None:
            logger.setLevel(default_level)
        else:
            logger.setLevel(level.upper() if isinstance(level, str) else level)
            logger.overridden = True

        return logger.level

    return _handler


def setup_logging_format(default_format="text"):
    """Create logging format configuration function.

    Returns a function meant for use as a CLI option callback. It sets the
    log line format for the elastic.pipes logger hierarchy and, when a
    format is explicitly passed (as opposed to left at its default), flags
    the choice as CLI-overridden so that a 'logging.format' pipe config
    node is ignored (see the 'elastic.pipes' pipe).

    Args:
        default_format: Format to apply if the option is not passed.

    Returns:
        Function that takes a format name argument and configures the
        elastic.pipes logger hierarchy.

    Raises:
        ConfigError: If an explicitly passed format is not recognized.
    """

    def _format(fmt):
        import logging

        if fmt is None:
            set_logging_format(default_format)
        else:
            set_logging_format(fmt)
            logging.getLogger("elastic.pipes").format_overridden = True

        return fmt

    return _format


def walk_tree(value, path=None):
    """Recursively walk dictionary tree, yielding (path, value) tuples.

    Args:
        value: Dictionary or other value to walk.
        path: Current path as list of keys.

    Yields:
        Tuples of (path_list, leaf_value) for non-mapping values.
    """
    if path is None:
        path = []
    if isinstance(value, Mapping):
        for k, v in value.items():
            yield from walk_tree(v, path + [k])
    else:
        yield path, value


def walk_contexts(pipe):
    """Walk all Context classes used by pipe.

    Args:
        pipe: Pipe instance to inspect.

    Yields:
        Context classes from function parameters and nested contexts.
    """
    from inspect import signature

    from . import Pipe

    def _walk_ann(ann):
        if isinstance(ann, type) and issubclass(ann, Pipe.Context):
            yield ann
            yield from _walk_context(ann)

    def _walk_context(ctx):
        try:
            hints = get_type_hints(ctx, include_extras=True)
        except NameError as e:
            raise Error(f"cannot resolve annotations for context '{ctx.__name__}': {e}") from e
        for ann in hints.values():
            yield from _walk_ann(ann)

    try:
        hints = get_type_hints(pipe.func, include_extras=True)
    except NameError as e:
        raise Error(f"cannot resolve annotations for pipe '{pipe.name}': {e}") from e
    for name, param in signature(pipe.func).parameters.items():
        yield from _walk_ann(hints.get(name, param.annotation))


def walk_params(pipe):
    """Walk all parameters with Node bindings in pipe.

    Args:
        pipe: Pipe instance to inspect.

    Yields:
        Tuples of (node, type, help, notes, default, empty) for each
        parameter bound to config or state node.
    """
    from inspect import signature

    from . import CommonContext, Pipe

    def _walk_ann(ann, default, empty):
        if isinstance(ann, type):
            if issubclass(ann, Pipe.Context):
                yield from _walk_context(ann)
            return

        node = None
        help = None
        notes = None
        args = get_args(ann)
        for arg in args:
            if isinstance(arg, Pipe.Node):
                node = arg
            if isinstance(arg, Pipe.Help):
                help = arg.help
            if isinstance(arg, Pipe.Notes):
                notes = arg.notes
        if node:
            yield node, args[0], help, notes, default, empty

    def _walk_context(ctx):
        try:
            hints = get_type_hints(ctx, include_extras=True)
        except NameError as e:
            raise Error(f"cannot resolve annotations for context '{ctx.__name__}': {e}") from e
        for name, ann in hints.items():
            default = getattr(ctx, name, NoDefault)
            yield from _walk_ann(ann, default, NoDefault)

    yield from _walk_context(CommonContext)

    try:
        hints = get_type_hints(pipe.func, include_extras=True)
    except NameError as e:
        raise Error(f"cannot resolve annotations for pipe '{pipe.name}': {e}") from e
    for name, param in signature(pipe.func).parameters.items():
        yield from _walk_ann(hints.get(name, param.annotation), param.default, param.empty)


def walk_config_nodes(pipes, prefix):
    """Visit config nodes that reference state nodes with given prefix.

    Args:
        pipes: List of (pipe, config) tuples.
        prefix: State node path prefix to match.

    Yields:
        Tuples of (pipe, 'config'/'state', node, help, notes, type, indirect, arg_name).
    """

    from . import Pipe, _indirect

    def _get_name(config, indirect):
        if has_node(config, _indirect(indirect)):
            node = get_node(config, _indirect(indirect))
            if isinstance(node, str) and node.startswith(prefix):
                return node

    def _scan_assembled(config, param_node):
        """Yield (key_name, arg_name) for @-keyed entries in an assembled dict whose values start with prefix."""
        if param_node is None:
            return
        config_val = get_node(config, param_node, None)
        if not isinstance(config_val, Mapping):
            return
        for k, v in config_val.items():
            if k.endswith("@") and isinstance(v, str) and v.startswith(prefix):
                yield f"{param_node}.{k}", v

    for pipe, config in pipes:
        for node, _type, help, notes, default, empty in walk_params(pipe):
            if isinstance(node, Pipe.Config):
                if node.indirect:
                    indirect = node.node if node.indirect is True else node.indirect
                    if arg_name := _get_name(config, indirect):
                        yield pipe, "config", node, help, notes, _type, indirect, arg_name
                for key_name, arg_name in _scan_assembled(config, node.node):
                    yield pipe, "config", node, help, notes, Any, key_name, arg_name
            elif isinstance(node, Pipe.State):
                if node.indirect:
                    indirect = node.node if node.indirect is True else node.indirect
                    if arg_name := _get_name(config, indirect):
                        yield pipe, "state", node, help, notes, _type, indirect, arg_name
                for key_name, arg_name in _scan_assembled(config, node.node):
                    yield pipe, "state", node, help, notes, Any, key_name, arg_name


def walk_args_env(pipes, args_env):
    """Visit config nodes that reference runtime arguments or environment variables.

    Args:
        pipes: List of (pipe, config) tuples.
        args_env: Either 'arguments' or 'environment'.

    Yields:
        Tuples of (name, type) for each argument/environment variable.
    """

    prefix = f"runtime.{args_env}."
    prefix_len = len(prefix)
    for _, _, _, _, _, _type, _, name in walk_config_nodes(pipes, prefix):
        yield name[prefix_len:], _type
