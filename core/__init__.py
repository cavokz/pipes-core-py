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

from typing_extensions import Annotated, Any, NoDefault, get_args

from .errors import ConfigError, Error
from .util import get_node, has_node, is_mutable, set_node

__version__ = "0.5.0-dev"


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

    def __init__(self, name, default=sys.exit):
        self.func = None
        self.name = name
        self.default = default
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
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

    def run(self, config, state, dry_run, logger):
        from inspect import signature

        kwargs = {}
        for name, param in signature(self.func).parameters.items():
            if name == "dry_run":
                kwargs["dry_run"] = dry_run
                continue
            if isinstance(param.annotation, type):
                if issubclass(param.annotation, Pipe):
                    kwargs[name] = self
                elif issubclass(param.annotation, logging.Logger):
                    kwargs[name] = self.logger
                elif issubclass(param.annotation, Pipe.Context):
                    kwargs[name] = param.annotation.bind(config, state, logger)
                continue
            args = get_args(param.annotation)
            for ann in args:
                if isinstance(ann, Pipe.Context):
                    param = Pipe.Context.Param(name, args[0], param.default, param.empty)
                    _, getter, _ = ann.handle_param(param, config, state, logger)
                    kwargs[name] = getter(None)

        if not dry_run or "dry_run" in kwargs:
            try:
                self.__config__ = config
                self.state = state
                return self.func(**kwargs)
            finally:
                del self.__config__
                del self.state

    def config(self, flag, default=NoDefault):
        return get_node(self.__config__, flag, default)

    def get_es(self):
        from .util import get_es_client

        stack = get_node(self.state, "stack")
        return get_es_client(stack)

    def get_kb(self):
        from .util import get_kb_client

        stack = get_node(self.state, "stack")
        return get_kb_client(stack)

    class Context(ABC):
        Param = namedtuple("Param", ["name", "type", "default", "empty"])
        Binding = namedtuple("Binding", ["node"])

        def __init__(self, node=None):
            self.node = node

        @classmethod
        def bind(cls, config, state, logger):
            # define a new sub-type of the user's context, make it concrete or it cannot be instantiated
            sub = type(cls.__name__, (cls,), {"handle_param": None})
            bindings = {}
            for name, ann in cls.__annotations__.items():
                if isinstance(ann, type):
                    if issubclass(ann, Pipe.Context):
                        nested = ann.bind(config, state, logger)
                        setattr(sub, name, nested)
                    continue
                args = get_args(ann)
                for i, ann in enumerate(args):
                    if isinstance(ann, Pipe.Context):
                        default = getattr(cls, name, NoDefault)
                        param = cls.Param(name, args[0], default, NoDefault)
                        node, getter, setter = ann.handle_param(param, config, state, logger)
                        setattr(sub, name, property(getter, setter))
                        bindings[name] = cls.Binding(node)

            setattr(sub, "__pipe_ctx_bindings__", bindings)
            return sub()

        @classmethod
        def get_binding(cls, name):
            return cls.__pipe_ctx_bindings__.get(name)

        @abstractmethod
        def handle_param(self, param, config, state, logger):
            pass

    class Config(Context):
        def handle_param(self, param, config, state, logger):
            if param.default is not param.empty and is_mutable(param.default):
                raise TypeError(f"mutable default config values are not allowed: {param.default}")
            has_value = has_node(config, self.node)
            has_indirect = has_node(config, _indirect(self.node))
            if has_value and has_indirect:
                raise ConfigError(f"cannot specify both '{self.node}' and '{_indirect(self.node)}'")
            if has_indirect:
                node = get_node(config, _indirect(self.node))
                root = state
                root_name = "state"
            else:
                node = self.node
                root = config
                root_name = "config"
            logger.debug(f"  bind context '{param.name}' to {root_name} node '{node}'")

            def default_action():
                if param.default is param.empty:
                    raise KeyError(f"{root_name} node not found: '{node}'")
                return param.default

            def getter(_):
                value = get_node(root, node, default_action=default_action)
                if value is None or param.type is Any or isinstance(value, param.type):
                    return value
                value_type = type(value).__name__
                expected_type = param.type.__name__
                raise Error(f"{root_name} node type mismatch: '{value_type}' (expected '{expected_type}')")

            return node, getter, None

    class State(Context):
        def __init__(self, node, *, indirect=True, mutable=False):
            super().__init__(node)
            self.indirect = indirect
            self.mutable = mutable
            if node is None and not isinstance(indirect, str):
                self.indirect = False

        def handle_param(self, param, config, state, logger):
            if param.default is not param.empty and is_mutable(param.default):
                raise TypeError(f"mutable default state values are not allowed: {param.default}")
            if self.indirect:
                indirect = _indirect(self.node if self.indirect is True else self.indirect)
                has_indirect = has_node(config, indirect)
            else:
                has_indirect = False
            node = get_node(config, indirect) if has_indirect else self.node
            if node is None:
                logger.debug(f"  bind context '{param.name}' to the whole state")
            else:
                logger.debug(f"  bind context '{param.name}' to state node '{node}'")

            def default_action():
                if param.default is param.empty:
                    raise KeyError(f"state node not found: '{node}'")
                return param.default

            def getter(_):
                value = get_node(state, node, default_action=default_action)
                if value is None or param.type is Any or isinstance(value, param.type):
                    return value
                value_type = type(value).__name__
                expected_type = param.type.__name__
                raise Error(f"state node type mismatch: '{value_type}' (expected '{expected_type}')")

            def setter(_, value):
                set_node(state, node, value)

            return node, getter, setter if self.mutable else None


@Pipe("elastic.pipes")
def _elastic_pipes(
    log: logging.Logger,
    level: Annotated[str, Pipe.Config("logging.level")] = None,
    min_version: Annotated[str, Pipe.Config("minimum-version")] = None,
    dry_run: bool = False,
):
    if level is not None and not getattr(log, "overridden", False):
        log.setLevel(level.upper())
    if min_version is not None:
        from semver import VersionInfo

        if VersionInfo.parse(__version__) < VersionInfo.parse(min_version):
            raise ConfigError(f"current version is older than minimum version: {__version__} < {min_version}")
