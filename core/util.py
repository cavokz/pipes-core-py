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


def get_es_client(stack):
    """Create Elasticsearch client from configuration.

    Args:
        stack: Configuration dictionary containing elasticsearch.url and
            credentials (api-key or username/password).

    Returns:
        Configured Elasticsearch client instance.
    """
    from elasticsearch import Elasticsearch

    shell_expand = get_node(stack, "shell-expand", False)
    api_key = get_node(stack, "credentials.api-key", None, shell_expand=shell_expand)
    username = get_node(stack, "credentials.username", None, shell_expand=shell_expand)
    password = get_node(stack, "credentials.password", None, shell_expand=shell_expand)

    args = {
        "hosts": get_node(stack, "elasticsearch.url", shell_expand=shell_expand),
    }
    if api_key:
        args["api_key"] = api_key
    elif username:
        args["basic_auth"] = (username, password)
    return Elasticsearch(**args)


def get_kb_client(stack):
    """Create Kibana client from configuration.

    Args:
        stack: Configuration dictionary containing kibana.url and
            credentials (api-key or username/password).

    Returns:
        Configured Kibana client instance.
    """
    from .kibana import Kibana

    shell_expand = get_node(stack, "shell-expand", False)
    api_key = get_node(stack, "credentials.api-key", None, shell_expand=shell_expand)
    username = get_node(stack, "credentials.username", None, shell_expand=shell_expand)
    password = get_node(stack, "credentials.password", None, shell_expand=shell_expand)

    args = {
        "url": get_node(stack, "kibana.url", shell_expand=shell_expand),
    }
    if api_key:
        args["api_key"] = api_key
    elif username:
        args["basic_auth"] = (username, password)
    return Kibana(**args)


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

    Args:
        msg: Error message to display.
    """
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


def setup_logging(default_level="NOTSET"):
    """Create logging configuration function.

    Args:
        default_level: Default log level if not overridden.

    Returns:
        Function that takes a level argument and configures the
        elastic.pipes logger hierarchy.
    """

    def _handler(level):
        import logging

        formatter = logging.Formatter("%(name)s - %(message)s")

        # a single handler to rule them all
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        # root of all the elastic pipes loggers
        logger = logging.getLogger("elastic.pipes")

        # all the pipes sync their handlers with this
        logger.addHandler(handler)

        # all the pipes sync their log level with this, unless configured differently
        if level is None:
            logger.setLevel(default_level)
        else:
            logger.setLevel(level.upper() if isinstance(level, str) else level)
            logger.overridden = True

        return logger.level

    return _handler


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
