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
from collections.abc import Mapping, Sequence
from copy import deepcopy

from typing_extensions import Annotated, Any, NoDefault, get_args

from .errors import ConfigError, Error
from .util import get_node, set_node

__version__ = "0.4.0"


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
                    continue
                if issubclass(param.annotation, logging.Logger):
                    kwargs[name] = self.logger
                    continue
            args = get_args(param.annotation)
            for ann in args:
                if isinstance(ann, self.Node):
                    ann_name = ann.__class__.__name__.lower()
                    root = locals()[ann_name]
                    node = ann.node
                    indirect = getattr(ann, "indirect", False)
                    if indirect:
                        if indirect is True:
                            indirect = node
                        node = get_node(config, indirect, None) or node
                    try:
                        logger.debug(f"  pass {ann_name} node '{node}' as variable '{name}'")
                        value = get_node(root, node)
                        if args[0] is not Any:
                            logger.debug(f"    checking value type is a '{args[0].__name__}'")
                            if not isinstance(value, args[0]):
                                raise Error(f"{ann_name} node type mismatch: '{type(value).__name__}' (expected '{args[0].__name__}')")
                        kwargs[name] = value
                    except KeyError:
                        if param.default is param.empty:
                            raise KeyError(f"{ann_name} node not found: '{node}'")
                        logger.debug(f"    copying default value '{param.default}'")
                        default = deepcopy(param.default)
                        if getattr(ann, "setdefault", False):
                            logger.debug("    setting node to default value")
                            set_node(root, node, default)
                        kwargs[name] = default

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

    class Node:
        def __init__(self, node):
            self.node = node

    class Config(Node):
        pass

    class State(Node):
        def __init__(self, node, *, setdefault=False, indirect=True):
            super().__init__(node)
            self.setdefault = setdefault
            self.indirect = indirect


@Pipe("elastic.pipes")
def elastic_pipes(
    pipe: Pipe,
    dry_run: bool = False,
    level: Annotated[str, Pipe.Config("logging.level")] = None,
    min_version: Annotated[str, Pipe.Config("minimum-version")] = None,
):
    if level is not None and not getattr(pipe.logger, "overridden", False):
        pipe.logger.setLevel(level.upper())
    if min_version is not None:
        from semver import VersionInfo

        if VersionInfo.parse(__version__) < VersionInfo.parse(min_version):
            raise ConfigError(f"invalid configuration: current version is older than minimum version: {__version__} < {min_version}")
