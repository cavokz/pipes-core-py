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

"""Core definitions for creating Elastic Pipes components."""
import logging
import sys
from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from typing import ClassVar

from typing_extensions import Annotated, Any, NoDefault, get_args, get_type_hints

from .errors import ConfigError, Error
from .util import (
    get_node,
    has_node,
    is_mutable,
    quote_path_segment,
    set_node,
    split_path,
)

__version__ = "0.8.0-dev"

TRACE = 5
logging.addLevelName(TRACE, "TRACE")


def _indirect(node):
    """Generate indirect reference node name by appending '@' to the final path segment.

    Appending '@' to the raw path string would break paths whose final
    segment is quoted (e.g. 'data."a.b"' would become the invalid
    'data."a.b"@'). Splitting and re-quoting as needed keeps the result a
    valid path regardless of whether the final segment needs quoting.
    """
    segments = list(split_path(node))
    segments[-1] += "@"
    return ".".join(quote_path_segment(s) for s in segments)


def validate_logging_config(name, config):
    if level := get_node(config, "logging.level", None):
        level_nr = logging.getLevelName(level.upper())
        if not isinstance(level_nr, int):
            raise ConfigError(f"invalid configuration: pipe '{name}': node 'logging.level': value '{level}'")


def get_pipes(state):
    if state is None:
        state = {}
    if not isinstance(state, Mapping):
        raise ConfigError(f"invalid state: not a mapping: {state} ({type(state).__name__})")
    pipes = state.get("pipes", [])
    if pipes is None:
        pipes = []
    if not isinstance(pipes, Sequence):
        raise ConfigError(f"invalid pipes configuration: not a sequence: {pipes} ({type(pipes).__name__})")
    configs = []
    for pipe in pipes:
        if not isinstance(pipe, Mapping):
            raise ConfigError(f"invalid pipe configuration: not a mapping: {pipe} ({type(pipe).__name__})")
        if len(pipe) != 1:
            raise ConfigError(f"invalid pipe configuration: multiple pipe names: {', '.join(pipe)}")
        name = set(pipe).pop()
        config = pipe.get(name)
        if config is None:
            config = {}
        if not isinstance(config, Mapping):
            raise ConfigError(f"invalid pipe configuration: not a mapping: {config} ({type(config).__name__})")
        validate_logging_config(name, config)
        configs.append((name, config))
    return configs


def _get_name_from_func(func):
    if func.__module__ != "__main__":
        return func.__module__

    import inspect
    from pathlib import Path

    file_path = inspect.getfile(func)
    return Path(file_path).stem


class Pipe:
    __pipes__: ClassVar[dict] = {}

    def __init__(self, name=None, *, default=sys.exit, notes=None, closing_notes=None):
        self.func = None
        self.name = name
        self.notes = notes
        self.closing_notes = closing_notes
        self.default = default
        self.logger = None

    def __call__(self, func):
        from functools import partial, update_wrapper

        from .standalone import run

        if self.name is None:
            self.name = _get_name_from_func(func)

        self.logger = logging.getLogger(self.name)
        self.logger.propagate = False

        if self.name in self.__pipes__:
            module = self.__pipes__[self.name].func.__module__
            raise ConfigError(f"pipe '{self.name}' is already defined in module '{module}'")

        self.__pipes__[self.name] = self
        self.func = func
        wrapped = partial(run, self)
        update_wrapper(wrapped, func)
        return wrapped

    @classmethod
    def find(cls, name):
        return cls.__pipes__[name]

    def _walk_config_params(self):
        from .util import walk_params

        for node, type_, *_ in walk_params(self):
            if isinstance(node, Pipe.Config):
                yield node.node, type_
                if indirect := node.get_indirect_node_name():
                    yield indirect, str
            elif isinstance(node, Pipe.State):
                if indirect := node.get_indirect_node_name():
                    yield indirect, str
                if node.node is not None and (type_ is Any or (isinstance(type_, type) and issubclass(type_, Mapping))):
                    yield node.node, type_

    def check_config(self, config, core_logger):
        from .util import split_path, walk_tree

        core_logger.debug(f"checking configuration '{self.name}'...")

        params = list(self._walk_config_params())
        nodes = [path for path, _ in walk_tree(config)]

        unknown = set()
        for node_path in nodes:
            for param, type_ in params:
                param_path = split_path(param)
                if node_path == param_path:
                    break
                if (
                    (type_ is Any or (isinstance(type_, type) and issubclass(type_, Mapping)))
                    and len(param_path) < len(node_path)
                    and all(a == b for a, b in zip(param_path, node_path))
                ):
                    break
            else:
                unknown.add(".".join(node_path))

        if unknown:
            nodes = "nodes" if len(unknown) > 1 else "node"
            unknown = "', '".join(sorted(unknown))
            raise ConfigError(f"unknown config {nodes}: '{unknown}'")

    def run(self, config, state, dry_run, core_logger, exit_stack):
        from inspect import signature

        core_logger.debug(f"executing pipe '{self.name}'...")

        with ExitStack() as stack:
            cc = CommonContext.bind(stack, config, state, core_logger, self.logger)

            try:
                hints = get_type_hints(self.func, include_extras=True)
            except NameError as e:
                raise Error(f"cannot resolve annotations for pipe '{self.name}': {e}") from e
            kwargs = {}
            for name, param in signature(self.func).parameters.items():
                if name == "dry_run":
                    kwargs["dry_run"] = dry_run
                    continue
                ann = hints.get(name, param.annotation)
                if isinstance(ann, type):
                    if issubclass(ann, Pipe):
                        kwargs[name] = self
                    elif issubclass(ann, logging.Logger):
                        kwargs[name] = self.logger
                    elif issubclass(ann, ExitStack):
                        kwargs[name] = exit_stack
                    elif issubclass(ann, Pipe.Context):
                        kwargs[name] = ann.bind(stack, config, state, core_logger, self.logger)
                    elif issubclass(ann, CommonContext):
                        kwargs[name] = cc
                    continue
                args = get_args(ann)
                for ann in args:
                    if isinstance(ann, Pipe.Node):
                        param = Pipe.Node.Param(name, args[0], param.default, param.empty)
                        _, getter, _ = ann.handle_param(param, config, state, core_logger)
                        try:
                            kwargs[name] = getter(None)
                        except KeyError as e:
                            raise Error(e.args[0])

            return self.func(**kwargs)

    class Help:
        def __init__(self, help):
            self.help = help

    class Notes:
        def __init__(self, notes):
            self.notes = notes

    class Context:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        @classmethod
        def bind(cls, stack, config, state, core_logger, pipe_logger):
            # define a new sub-type of the user's context
            sub = type(cls.__name__, (cls,), {"logger": pipe_logger})
            bindings = {}
            try:
                hints = get_type_hints(cls, include_extras=True)
            except NameError as e:
                raise Error(f"cannot resolve annotations for context '{cls.__name__}': {e}") from e
            for name, ann in hints.items():
                if isinstance(ann, type):
                    if issubclass(ann, Pipe.Context):
                        nested = ann.bind(stack, config, state, core_logger, pipe_logger)
                        setattr(sub, name, nested)
                    continue
                args = get_args(ann)
                for i, ann in enumerate(args):
                    if isinstance(ann, Pipe.Node):
                        default = getattr(cls, name, NoDefault)
                        param = Pipe.Node.Param(name, args[0], default, NoDefault)
                        binding, getter, setter = ann.handle_param(param, config, state, core_logger)
                        setattr(sub, name, property(getter, setter))
                        bindings[name] = binding
                        try:
                            getter(None)
                        except KeyError as e:
                            raise Error(e.args[0])

            sub.__pipe_ctx_bindings__ = bindings
            return stack.enter_context(sub())

        @classmethod
        def get_binding(cls, name):
            return cls.__pipe_ctx_bindings__.get(name)

    class Node(ABC):
        Param = namedtuple("Param", ["name", "type", "default", "empty"])

        class Binding:
            node: str
            root: dict
            root_name: str

        def __init__(self, node):
            self.node = node

        @abstractmethod
        def handle_param(self, param, config, state, core_logger):
            pass

    class Config(Node):
        def __init__(self, node, *, indirect=True):
            super().__init__(node)
            self.indirect = indirect

        def get_indirect_node_name(self):
            if self.indirect:
                return _indirect(self.node if self.indirect is True else self.indirect)

        def handle_param(self, param, config, state, core_logger):
            if param.default is not param.empty and is_mutable(param.default):
                raise TypeError(f"param '{param.name}': mutable default not allowed: {param.default}")
            indirect = self.get_indirect_node_name()

            binding = Pipe.Node.Binding()
            binding.node = self.node
            binding.assembly_config = None
            binding.root = None

            if has_node(config, self.node):
                if indirect and has_node(config, indirect):
                    raise ConfigError(f"param '{param.name}': config cannot specify both '{self.node}' and '{indirect}'")
                value = get_node(config, self.node)
                if isinstance(value, Mapping):
                    if any(isinstance(k, str) and k.endswith("@") for k in value):
                        binding.assembly_config = value
                        core_logger.debug(f"  bind param '{param.name}' to assembled dict from config node '{self.node}'")
                    else:
                        core_logger.debug(f"  bind param '{param.name}' to literal dict from config node '{self.node}'")
                binding.root = config
                binding.root_name = "config"

            if indirect and binding.root is None and has_node(config, indirect):
                binding.node = get_node(config, indirect)
                binding.root = state
                binding.root_name = "state"
                core_logger.debug(f"  bind param '{param.name}' to state node '{binding.node}'")

            if binding.root is None:
                binding.root = config
                binding.root_name = "config"
                core_logger.debug(f"  bind param '{param.name}' to config node '{self.node}'")

            def default_action():
                if param.default is param.empty:
                    raise KeyError(f"param '{param.name}': {binding.root_name} node not found: '{binding.node}'")
                return param.default

            def getter(_):
                if binding.assembly_config is None:
                    value = get_node(binding.root, binding.node, default_action=default_action)
                else:
                    value = {}
                    for k, v in binding.assembly_config.items():
                        if k.endswith("@"):
                            try:
                                value[k[:-1]] = get_node(state, v)
                            except KeyError:
                                raise KeyError(f"param '{param.name}': state node not found: '{v}'")
                        else:
                            value[k] = v
                if value is None or param.type is Any or isinstance(value, param.type):
                    return value
                value_type = type(value).__name__
                expected_type = param.type.__name__
                raise Error(
                    f"param '{param.name}': {binding.root_name} node '{binding.node}' type mismatch: '{value_type}' (expected '{expected_type}')"
                )

            def setter(_, value):
                if binding.node != self.node or binding.root is not config or binding.root_name != "config":
                    binding.node = self.node
                    binding.root = config
                    binding.root_name = "config"
                    core_logger.debug(f"  re-bind param '{param.name}' to {binding.root_name} node '{binding.node}'")
                    if indirect:
                        config.pop(indirect)
                binding.assembly_config = None
                set_node(binding.root, binding.node, value)

            return binding, getter, setter

    class State(Node):
        def __init__(self, node, *, indirect=True, mutable=False):
            super().__init__(node)
            self.indirect = indirect
            self.mutable = mutable
            if node is None and not isinstance(indirect, str):
                self.indirect = False
            if node is not None and node.startswith("runtime."):
                self.indirect = False

        def get_indirect_node_name(self):
            if self.indirect:
                return _indirect(self.node if self.indirect is True else self.indirect)

        def handle_param(self, param, config, state, core_logger):
            if param.default is not param.empty and is_mutable(param.default):
                raise TypeError(f"param '{param.name}': mutable default not allowed: {param.default}")
            node = self.node
            indirect = self.get_indirect_node_name()

            binding = Pipe.Node.Binding()
            binding.node = node
            binding.assembly_config = None
            binding.root = None

            if node is not None and has_node(config, node):
                if indirect and has_node(config, indirect):
                    raise ConfigError(f"param '{param.name}': config cannot specify both '{node}' and '{indirect}'")
                value = get_node(config, node, None)
                if not isinstance(value, Mapping):
                    raise ConfigError(f"param '{param.name}': config node '{node}' must be a mapping")
                if any(isinstance(k, str) and k.endswith("@") for k in value):
                    binding.assembly_config = value
                    core_logger.debug(f"  bind param '{param.name}' to assembled dict from config node '{node}'")
                else:
                    core_logger.debug(f"  bind param '{param.name}' to literal dict from config node '{node}'")
                binding.root = config
                binding.root_name = "config"

            if indirect and binding.root is None:
                node = get_node(config, indirect, node)
                binding.node = node

            if binding.root is None:
                binding.root = state
                binding.root_name = "state"
                if node is None:
                    core_logger.debug(f"  bind param '{param.name}' to the whole state")
                else:
                    core_logger.debug(f"  bind param '{param.name}' to state node '{node}'")

            def default_action():
                if param.default is param.empty:
                    raise KeyError(f"param '{param.name}': {binding.root_name} node not found: '{binding.node}'")
                return param.default

            def getter(_):
                if binding.assembly_config is None:
                    value = get_node(binding.root, binding.node, default_action=default_action)
                else:
                    value = {}
                    for k, v in binding.assembly_config.items():
                        if k.endswith("@"):
                            try:
                                value[k[:-1]] = get_node(state, v)
                            except KeyError:
                                raise KeyError(f"param '{param.name}': state node not found: '{v}'")
                        else:
                            value[k] = v
                if value is not None and is_mutable(value) and not self.mutable:
                    raise AttributeError(f"param '{param.name}' is mutable but not marked as such")
                if value is None or param.type is Any or isinstance(value, param.type):
                    return value
                value_type = type(value).__name__
                expected_type = param.type.__name__
                raise Error(
                    f"param '{param.name}': {binding.root_name} node '{binding.node}' type mismatch: '{value_type}' (expected '{expected_type}')"
                )

            def setter(_, value):
                if not self.mutable:
                    raise AttributeError(f"param '{param.name}' is not mutable")
                if binding.node != node or binding.root is not state or binding.root_name != "state":
                    binding.node = node
                    binding.root = state
                    binding.root_name = "state"
                    core_logger.debug(f"  re-bind param '{param.name}' to {binding.root_name} node '{binding.node}'")
                binding.assembly_config = None
                set_node(binding.root, binding.node, value)

            return binding, getter, setter


class CommonContext(Pipe.Context):
    logging_level: Annotated[
        str,
        Pipe.Config("logging.level"),
        Pipe.Help("emit logging messages at such severity or higher"),
        Pipe.Notes("default: 'debug' if in UNIX pipe mode, 'info' otherwise"),
    ] = None

    def __init__(self):
        elastic_pipes_logger = logging.getLogger("elastic.pipes")
        if self.logger is not elastic_pipes_logger:
            for handler in reversed(self.logger.handlers):
                self.logger.removeHandler(handler)
            for handler in elastic_pipes_logger.handlers:
                self.logger.addHandler(handler)
        if self.logging_level is None or getattr(elastic_pipes_logger, "overridden", False):
            self.logger.setLevel(elastic_pipes_logger.level)
        else:
            self.logger.setLevel(self.logging_level.upper())


@Pipe("elastic.pipes")
def _elastic_pipes(
    min_version: Annotated[
        str,
        Pipe.Config("minimum-version"),
    ] = None,
    search_path: Annotated[
        Sequence,
        Pipe.Config("search-path"),
    ] = None,
    dry_run: bool = False,
):
    if min_version is not None:
        from semver import VersionInfo

        if VersionInfo.parse(__version__) < VersionInfo.parse(min_version):
            raise ConfigError(f"current version is older than minimum version: {__version__} < {min_version}")
