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
import os
import sys

from .errors import ConfigError, Error
from .util import deserialize_yaml, get_field, serialize_yaml


class __no_default__:
    pass


def validate_logging_config(name, config):
    if level := get_field(config, "logging.level"):
        level_nr = getattr(logging, level.upper(), None)
        if not isinstance(level_nr, int):
            raise ConfigError(f"invalid configuration: pipe '{name}': field 'logging.level': value '{level}'")


def get_pipes(state):
    if not isinstance(state, dict):
        raise ConfigError(f"invalid state: not a map: {state} ({type(state).__name__})")
    pipes = state.get("pipes", [])
    if not isinstance(pipes, list):
        raise ConfigError(f"invalid configuration: not a list: {pipes} ({type(pipes).__name__})")
    configs = []
    for pipe in pipes:
        if not isinstance(pipe, dict):
            raise ConfigError(f"invalid configuration: not a map: {pipe} ({type(pipe).__name__})")
        if len(pipe) != 1:
            raise ConfigError(f"invalid configuration: multiple pipe names: {', '.join(pipe)}")
        name = set(pipe).pop()
        config = pipe.get(name) or {}
        validate_logging_config(name, config)
        configs.append((name, config))
    return configs


def __sync_logger_config__(pipe):
    elastic_pipes_logger = logging.getLogger("elastic.pipes")
    if pipe.logger == elastic_pipes_logger:
        return
    for handler in reversed(pipe.logger.handlers):
        pipe.logger.removeHandler(handler)
    for handler in elastic_pipes_logger.handlers:
        pipe.logger.addHandler(handler)
    level = pipe.config("logging.level", None)
    if level is None:
        pipe.logger.setLevel(elastic_pipes_logger.level)
    else:
        pipe.logger.setLevel(level.upper())


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
        if self.name in self.__pipes__:
            module = self.__pipes__[self.name].func.__module__
            raise ConfigError(f"pipe '{self.name}' is already defined in module '{module}'")

        self.__pipes__[self.name] = self
        self.func = func
        return wrap_standalone_pipe(self)

    @classmethod
    def run(cls, state, *, dry_run=False):
        from importlib import import_module
        from inspect import signature

        if not state:
            raise ConfigError("invalid configuration, it's empty")

        logger = logging.getLogger("elastic.pipes.core")

        pipes = get_pipes(state)

        for name, config in pipes:
            if name in cls.__pipes__:
                continue
            logger.debug(f"loading pipe '{name}'...")
            import_module(name)
            if name not in cls.__pipes__:
                raise ConfigError(f"module does not define a pipe: {name}")

        for name, config in pipes:
            pipe = cls.__pipes__[name]
            pipe.__config__ = config
            pipe.state = state
            __sync_logger_config__(pipe)
            sig = signature(pipe.func)
            if "dry_run" in sig.parameters:
                if dry_run:
                    logger.debug(f"dry executing pipe '{name}'...")
                else:
                    logger.debug(f"executing pipe '{name}'...")
                pipe.func(pipe, dry_run=dry_run)
            elif dry_run:
                logger.debug(f"not executing pipe '{name}'...")
            else:
                logger.debug(f"executing pipe '{name}'...")
                pipe.func(pipe)
            del pipe.state
            del pipe.__config__

    def config(self, flag, default=__no_default__):
        value = get_field(self.__config__, flag)
        if value is not None:
            return value
        if default is __no_default__:
            raise KeyError(flag)
        return default

    def get_es(self):
        from elasticsearch import Elasticsearch

        shell_expand = get_field(self.state, "stack.shell-expand")
        api_key = get_field(self.state, "stack.credentials.api-key", shell_expand=shell_expand)
        username = get_field(self.state, "stack.credentials.username", shell_expand=shell_expand)
        password = get_field(self.state, "stack.credentials.password", shell_expand=shell_expand)

        args = {
            "hosts": get_field(self.state, "stack.elasticsearch.url", shell_expand=shell_expand),
        }
        if api_key:
            args["api_key"] = api_key
        elif username:
            args["basic_auth"] = (username, password)
        return Elasticsearch(**args)

    def get_kb(self):
        from .kibana import Kibana

        shell_expand = get_field(self.state, "stack.shell-expand")
        api_key = get_field(self.state, "stack.credentials.api-key", shell_expand=shell_expand)
        username = get_field(self.state, "stack.credentials.username", shell_expand=shell_expand)
        password = get_field(self.state, "stack.credentials.password", shell_expand=shell_expand)

        args = {
            "url": get_field(self.state, "stack.kibana.url", shell_expand=shell_expand),
        }
        if api_key:
            args["api_key"] = api_key
        elif username:
            args["basic_auth"] = (username, password)
        return Kibana(**args)


def state_from_unix_pipe(logger, default):
    logger.debug("awaiting state from standard input")
    if sys.stdin.isatty():
        if os.name == "nt":
            print("Press CTRL-Z and ENTER to end", file=sys.stderr)
        else:
            print("Press CTRL-D one time (or two, if you entered any input) to end", file=sys.stderr)
    state = deserialize_yaml(sys.stdin)

    if state:
        logger.debug("got state")
    elif default is sys.exit:
        logger.debug("no state, exiting")
        sys.exit(1)
    else:
        logger.debug("using default state")
        state = default

    return state


def state_to_unix_pipe(logger, state):
    logger.debug("relaying state to standard output")
    serialize_yaml(sys.stdout, state)


def wrap_standalone_pipe(pipe):
    from functools import wraps

    @wraps(pipe.func)
    def _func(*args, **kwargs):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(name)s - %(message)s"))
        pipe.logger.addHandler(handler)
        pipe.logger.setLevel(logging.DEBUG)

        try:
            pipe.state = state_from_unix_pipe(pipe.logger, pipe.default)
            pipes = get_pipes(pipe.state)
        except Error as e:
            print(e, file=sys.stderr)
            sys.exit(1)

        pipe.__config__ = {}
        for name, config in pipes:
            if pipe.name == name:
                pipe.__config__ = config
                break

        ret = pipe.func(pipe, *args, **kwargs)
        state_to_unix_pipe(pipe.logger, pipe.state)
        return ret

    return _func


@Pipe("elastic.pipes")
def elastic_pipes(pipe, dry_run=False):
    level = pipe.config("logging.level", None)
    if level is not None:
        pipe.logger.setLevel(level.upper())
