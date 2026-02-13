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

"""Helper functions for the Elastic Pipes implementation."""

import os
import re
import sys
from collections.abc import Mapping

from typing_extensions import NoDefault, get_args

from .errors import ConfigError, Error

if sys.version_info >= (3, 12):
    from itertools import batched
else:
    from itertools import islice

    def batched(iterable, chunk_size):
        iterator = iter(iterable)
        while chunk := list(islice(iterator, chunk_size)):
            yield chunk


# Pattern to match valid paths: segments separated by dots
# Segment can be either '[^']+' or "[^"]+" (quoted) or [^.'"]+ (unquoted, excluding dots and quotes)
_valid_path_pattern = re.compile(r"""^('[^']+'|"[^"]+"|[^.'"]+)(\.('[^']+'|"[^"]+"|[^.'"]+))*$""")

# Extract parts: either single-quoted (group 1), double-quoted (group 2), or unquoted segments (group 3)
_extract_path_pattern = re.compile(r"""'([^']+)'|"([^"]+)"|([^.'"]+)""")


def get_es_client(stack):
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
    if path is None:
        return ()
    if not isinstance(path, str):
        raise Error(f"invalid path: type is '{type(path).__name__}' (expected 'str')")
    if not _valid_path_pattern.match(path):
        raise Error(f"invalid path: {path}")
    matches = _extract_path_pattern.findall(path)
    return [m[0] if m[0] else (m[1] if m[1] else m[2]) for m in matches]


def has_node(dict, path):
    keys = split_path(path)
    for key in keys:
        if not isinstance(dict, Mapping):
            return False
        try:
            dict = dict[key]
        except KeyError:
            return False
    return dict


def get_node(dict, path, default=NoDefault, *, default_action=None, shell_expand=False):
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
    import yaml

    try:
        # use the LibYAML C library bindings if available
        from yaml import CDumper as Dumper
    except ImportError:
        from yaml import Dumper

    yaml.dump(state, file, Dumper=Dumper)


def deserialize_yaml(file, *, streaming=False):
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
    import json

    file.write(json.dumps(state) + "\n")


def deserialize_json(file, *, streaming=False):
    import json

    if streaming:
        raise ConfigError("cannot stream json (try ndjson)")

    return json.load(file)


def serialize_ndjson(file, state):
    import json

    for elem in state:
        file.write(json.dumps(elem) + "\n")


def deserialize_ndjson(file, *, streaming=False):
    import json

    if streaming:
        return (json.loads(line) for line in file)

    return [json.loads(line) for line in file]


def serialize(file, state, *, format):
    if format in ("yaml", "yml"):
        serialize_yaml(file, state)
    elif format == "json":
        serialize_json(file, state)
    elif format == "ndjson":
        serialize_ndjson(file, state)
    else:
        raise ConfigError(f"unsupported format: {format}")


def deserialize(file, *, format, streaming=False):
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
    sys.exit(msg)


def warn_interactive(f):
    if f.isatty():
        if os.name == "nt":
            print("Press CTRL-Z and ENTER to end", file=sys.stderr)
        else:
            print("Press CTRL-D one time (or two, if you entered any input) to end", file=sys.stderr)


def is_mutable(value):
    d = {}
    try:
        d[value] = None
    except TypeError:
        return True
    return False


def setup_logging(default_level="NOTSET"):
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


def walk_tree(value, path=[]):
    if isinstance(value, Mapping):
        for k, v in value.items():
            yield from walk_tree(v, path + [k])
    else:
        yield path, value


def walk_contexts(pipe):
    from inspect import signature

    from . import Pipe

    def _walk_ann(ann):
        if isinstance(ann, type):
            if issubclass(ann, Pipe.Context):
                yield ann
                yield from _walk_context(ann)

    def _walk_context(ctx):
        for ann in ctx.__annotations__.values():
            yield from _walk_ann(ann)

    for param in signature(pipe.func).parameters.values():
        yield from _walk_ann(param.annotation)


def walk_params(pipe):
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
        for name, ann in ctx.__annotations__.items():
            default = getattr(ctx, name, NoDefault)
            yield from _walk_ann(ann, default, NoDefault)

    yield from _walk_context(CommonContext)

    for param in signature(pipe.func).parameters.values():
        yield from _walk_ann(param.annotation, param.default, param.empty)


def walk_config_nodes(pipes, prefix):
    """Visit all the config nodes that refer to a state node having the given prefix."""

    from . import Pipe, _indirect

    def _get_name(config, indirect):
        if has_node(config, _indirect(indirect)):
            node = get_node(config, _indirect(indirect))
            if node.startswith(prefix):
                return node

    for pipe, config in pipes:
        for node, _type, help, notes, default, empty in walk_params(pipe):
            if isinstance(node, Pipe.Config):
                indirect = node.node
                if arg_name := _get_name(config, indirect):
                    yield pipe, "config", node, help, notes, _type, indirect, arg_name
            elif isinstance(node, Pipe.State) and node.indirect:
                indirect = node.node if node.indirect is True else node.indirect
                if arg_name := _get_name(config, indirect):
                    yield pipe, "state", node, help, notes, _type, indirect, arg_name


def walk_args_env(pipes, args_env):
    """Visit all the config nodes that refer to an argument or an environment variable."""

    prefix = f"runtime.{args_env}."
    prefix_len = len(prefix)
    for _, _, _, _, _, _type, _, name in walk_config_nodes(pipes, prefix):
        yield name[prefix_len:], _type
