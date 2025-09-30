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

from typing_extensions import Annotated, Any, NoDefault, get_args

from .errors import ConfigError, Error
from .util import get_node, has_node, is_mutable, set_node

__version__ = "0.7.0"


def _indirect(node):
    return node + "@"


def validate_logging_config(name, config):
    if level := get_node(config, "logging.level", None):
        level_nr = getattr(logging, level.upper(), None)
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


class Pipe:
    __pipes__ = {}

    def __init__(self, name, *, default=sys.exit, notes=None, closing_notes=None):
        self.func = None
        self.name = name
        self.notes = notes
        self.closing_notes = closing_notes
        self.default = default
        self.logger = logging.getLogger(name)
        self.logger.propagate = False

    def __call__(self, func):
        from functools import partial

        from .standalone import run

        if self.name in self.__pipes__:
            module = self.__pipes__[self.name].func.__module__
            raise ConfigError(f"pipe '{self.name}' is already defined in module '{module}'")

        self.__pipes__[self.name] = self
        self.func = func
        return partial(run, self)

    @classmethod
    def find(cls, name):
        return cls.__pipes__[name]

    def _walk_config_params(self):
        from .util import walk_params

        for node, type_, *_ in walk_params(self):
            if isinstance(node, Pipe.Config):
                yield node.node, type_
                yield node.get_indirect_node_name(), str
            elif isinstance(node, Pipe.State):
                if indirect := node.get_indirect_node_name():
                    yield indirect, str

    def check_config(self, config):
        from .util import split_path, walk_tree

        params = list(self._walk_config_params())
        nodes = list(path for path, _ in walk_tree(config))

        unknown = set()
        for node_path in nodes:
            for param, type_ in params:
                param_path = split_path(param)
                if node_path == param_path:
                    break
                if issubclass(type_, Mapping) and len(param_path) < len(node_path) and all(a == b for a, b in zip(param_path, node_path)):
                    break
            else:
                unknown.add(".".join(node_path))

        if unknown:
            nodes = "nodes" if len(unknown) > 1 else "node"
            unknown = "', '".join(sorted(unknown))
            raise ConfigError(f"unknown config {nodes}: '{unknown}'")

    def run(self, config, state, dry_run, core_logger, exit_stack):
        from inspect import signature

        params = signature(self.func).parameters

        if not dry_run:
            core_logger.debug(f"executing pipe '{self.name}'...")
        elif "dry_run" in params:
            core_logger.debug(f"dry executing pipe '{self.name}'...")
        else:
            core_logger.debug(f"not executing pipe '{self.name}'...")

        with ExitStack() as stack:
            cc = CommonContext.bind(stack, config, state, core_logger, self.logger)

            kwargs = {}
            for name, param in params.items():
                if name == "dry_run":
                    kwargs["dry_run"] = dry_run
                    continue
                if isinstance(param.annotation, type):
                    if issubclass(param.annotation, Pipe):
                        kwargs[name] = self
                    elif issubclass(param.annotation, logging.Logger):
                        kwargs[name] = self.logger
                    elif issubclass(param.annotation, ExitStack):
                        kwargs[name] = exit_stack
                    elif issubclass(param.annotation, Pipe.Context):
                        kwargs[name] = param.annotation.bind(stack, config, state, core_logger, self.logger)
                    elif issubclass(param.annotation, CommonContext):
                        kwargs[name] = cc
                    continue
                args = get_args(param.annotation)
                for ann in args:
                    if isinstance(ann, Pipe.Node):
                        param = Pipe.Node.Param(name, args[0], param.default, param.empty)
                        _, getter, _ = ann.handle_param(param, config, state, core_logger)
                        try:
                            kwargs[name] = getter(None)
                        except KeyError as e:
                            raise Error(e.args[0])

            if not dry_run or "dry_run" in kwargs:
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
            for name, ann in cls.__annotations__.items():
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

            setattr(sub, "__pipe_ctx_bindings__", bindings)
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
        def get_indirect_node_name(self):
            return _indirect(self.node)

        def handle_param(self, param, config, state, core_logger):
            if param.default is not param.empty and is_mutable(param.default):
                raise TypeError(f"param '{param.name}': mutable default not allowed: {param.default}")
            indirect = self.get_indirect_node_name()
            has_value = has_node(config, self.node)
            has_indirect = has_node(config, indirect)
            if has_value and has_indirect:
                raise ConfigError(f"param '{param.name}': config cannot specify both '{self.node}' and '{indirect}'")
            binding = Pipe.Node.Binding()
            if has_indirect:
                binding.node = get_node(config, indirect)
                binding.root = state
                binding.root_name = "state"
            else:
                binding.node = self.node
                binding.root = config
                binding.root_name = "config"
            core_logger.debug(f"  bind param '{param.name}' to {binding.root_name} node '{binding.node}'")

            def default_action():
                if param.default is param.empty:
                    raise KeyError(f"param '{param.name}': {binding.root_name} node not found: '{binding.node}'")
                return param.default

            def getter(_):
                value = get_node(binding.root, binding.node, default_action=default_action)
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
                    config.pop(indirect)
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
            if indirect := self.get_indirect_node_name():
                node = get_node(config, indirect, node)
            if node is None:
                core_logger.debug(f"  bind param '{param.name}' to the whole state")
            else:
                core_logger.debug(f"  bind param '{param.name}' to state node '{node}'")

            binding = Pipe.Node.Binding()
            binding.node = node
            binding.root = state
            binding.root_name = "state"

            def default_action():
                if param.default is param.empty:
                    raise KeyError(f"param '{param.name}': {binding.root_name} node not found: '{binding.node}'")
                return param.default

            def getter(_):
                value = get_node(binding.root, binding.node, default_action=default_action)
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
