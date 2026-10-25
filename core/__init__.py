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

"""Pipe composition system.

Pipes are functions decorated with :class:`Pipe` that execute in sequence,
passing state through a shared dictionary. Each pipe can read from and write
to this state, allowing data flow between components.

Basic usage::

    from elastic.pipes.core import Pipe
    from typing_extensions import Annotated

    @Pipe("example.transform")
    def transform(
        input_data: Annotated[dict, Pipe.State("data", mutable=True)],
        processed: Annotated[dict, Pipe.State("processed", mutable=True)],
        threshold: Annotated[int, Pipe.Config("threshold")] = 10,
    ):
        # Process data and store results
        processed["count"] = len(input_data)

Pipes are composed in YAML configuration files::

    pipes:
      - example.transform:
          threshold: 20
      - elastic.pipes.core.export:
          file: output.yaml
          node@: processed

    data: {}
    processed: {}

See :class:`Pipe` for decorator details, :class:`Pipe.Config` and
:class:`Pipe.State` for parameter binding, and :class:`Pipe.Context`
for context managers.
"""
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

__version__ = "0.9.0-dev"

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
    """Validate logging.level and logging.format configuration values.

    Args:
        name: Pipe name for error messages.
        config: Configuration dictionary to validate.

    Raises:
        ConfigError: If logging.level or logging.format contains an invalid value.
    """
    if level := get_node(config, "logging.level", None):
        level_nr = logging.getLevelName(level.upper())
        if not isinstance(level_nr, int):
            raise ConfigError(f"invalid configuration: pipe '{name}': node 'logging.level': value '{level}'")
    if (fmt := get_node(config, "logging.format", None)) and (not isinstance(fmt, str) or fmt.lower() not in ("text", "json")):
        raise ConfigError(f"invalid configuration: pipe '{name}': node 'logging.format': value '{fmt}'")


def get_pipes(state):
    """Extract pipe configurations from state dictionary.

    Parses the 'pipes' key in state and returns a list of (name, config)
    tuples. Validates that each pipe entry is properly formatted as a
    single-key mapping.

    Args:
        state: State dictionary containing 'pipes' key with pipe configurations.
            Expected format::

                {
                    "pipes": [
                        {"pipe.name": {"config": "value"}},
                        {"another.pipe": {}},
                    ]
                }

    Returns:
        List of (name, config) tuples for each pipe.

    Raises:
        ConfigError: If state structure is invalid or pipe configurations
            are malformed.
    """
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
    """Decorator for creating pipeline components.

    Pipes are functions that execute in sequence, sharing state through a
    dictionary. Parameters are bound to config or state nodes using
    :class:`Pipe.Config` and :class:`Pipe.State` with ``Annotated`` type hints.

    Example::

        @Pipe("my.pipe")
        def process(
            input: Annotated[list, Pipe.State("input", mutable=True)],
            output: Annotated[list, Pipe.State("output", mutable=True)],
            log: logging.Logger,
            format: Annotated[str, Pipe.Config("format")] = "json",
        ):
            log.info(f"Processing {len(input)} items")
            output.extend(input)

    The pipe function can request:

    - Config values via ``Pipe.Config("node.path")``
    - State values via ``Pipe.State("node.path")``
    - Logger via ``logging.Logger`` type hint
    - Pipe instance via ``Pipe`` type hint
    - ExitStack via ``ExitStack`` type hint
    - Custom contexts via ``Pipe.Context`` subclasses
    - Dry-run flag via ``dry_run: bool`` parameter

    Attributes:
        name (str): Fully qualified pipe name.
        func (callable): Decorated function.
        logger (logging.Logger): Logger instance for this pipe.
        notes (str): Optional notes displayed before execution.
        closing_notes (str): Optional notes displayed after execution.
    """

    __pipes__: ClassVar[dict] = {}

    def __init__(self, name=None, *, default=sys.exit, notes=None, closing_notes=None):
        """Initialize pipe decorator.

        Args:
            name: Fully qualified pipe name. If omitted, infer it from the
                decorated function's module or script filename.
            default: Function called when pipe is run standalone without state.
                Defaults to sys.exit.
            notes: Optional notes to display before pipe execution.
            closing_notes: Optional notes to display after pipe execution.
        """
        self.func = None
        self.name = name
        self.notes = notes
        self.closing_notes = closing_notes
        self.default = default
        self.logger = None

    def __call__(self, func):
        """Register and wrap the decorated function.

        Args:
            func: Function to decorate as a pipe.

        Returns:
            Wrapped function that can be called standalone.

        Raises:
            ConfigError: If a pipe with this name is already registered.
        """
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
        """Find registered pipe by name.

        Args:
            name: Fully qualified pipe name.

        Returns:
            Pipe instance.

        Raises:
            KeyError: If pipe name is not registered.
        """
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
        """Validate configuration against pipe's declared parameters.

        Args:
            config: Configuration dictionary to validate.
            core_logger: Logger for core operations.

        Raises:
            ConfigError: If config contains unknown nodes.
        """
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
        """Execute pipe function with bound parameters.

        Binds function parameters to config/state nodes, injects contexts
        and resources, then always executes the function. dry_run is only
        forwarded to pipes that declare a ``dry_run`` parameter; it does not
        prevent execution on its own, so pipes that don't accept it run
        normally regardless of dry_run.

        Args:
            config: Pipe configuration dictionary.
            state: Shared state dictionary.
            dry_run: Forwarded to the pipe function if it accepts a dry_run
                parameter; otherwise has no effect here.
            core_logger: Logger for core operations.
            exit_stack: ExitStack for resource management.

        Returns:
            Function return value.

        Raises:
            Error: If required parameters are missing or type mismatches occur.
        """
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
        """Metadata for parameter help text.

        Used with ``Annotated`` to attach help text to parameters for
        documentation and CLI help generation.

        Example::

            threshold: Annotated[
                int,
                Pipe.Config("threshold"),
                Pipe.Help("minimum value to process"),
            ]
        """

        def __init__(self, help):
            """Initialize help metadata.

            Args:
                help: Help text string.
            """
            self.help = help

    class Notes:
        """Metadata for parameter notes.

        Used with ``Annotated`` to attach additional notes to parameters,
        typically describing defaults or special behaviors.

        Example::

            format: Annotated[
                str,
                Pipe.Config("format"),
                Pipe.Help("output format"),
                Pipe.Notes("default: guessed from file extension"),
            ]
        """

        def __init__(self, notes):
            """Initialize notes metadata.

            Args:
                notes: Notes text string.
            """
            self.notes = notes

    class Context:
        """Base class for pipe execution contexts.

        Context managers that group related parameters and provide structured
        access to configuration and state. Subclass and declare attributes
        with ``Annotated`` type hints to bind them to config/state nodes.

        Example::

            class ElasticsearchContext(Pipe.Context):
                url: Annotated[str, Pipe.Config("elasticsearch.url")]
                index: Annotated[str, Pipe.Config("index")]
                docs: Annotated[list, Pipe.State("documents", mutable=True)]

            @Pipe("example.es")
            def process(ctx: ElasticsearchContext, log: logging.Logger):
                log.info(f"Connecting to {ctx.url}")
                # ctx.docs can be modified
                ctx.docs.append({"new": "doc"})

        Attributes become properties bound to their respective config/state
        nodes. The context is automatically entered/exited during pipe execution.
        """

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        @classmethod
        def bind(cls, stack, config, state, core_logger, pipe_logger):
            """Create and bind context instance with resolved parameters.

            Args:
                stack: ExitStack to register context with.
                config: Configuration dictionary.
                state: State dictionary.
                core_logger: Logger for core operations.
                pipe_logger: Logger for pipe operations.

            Returns:
                Bound context instance entered in the stack.

            Raises:
                Error: If required parameters are missing.
            """
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
            """Get binding information for a context attribute.

            Args:
                name: Attribute name.

            Returns:
                Binding instance with node, root, and root_name attributes,
                or None if attribute is not bound.
            """
            return cls.__pipe_ctx_bindings__.get(name)

    class Node(ABC):
        """Abstract base for parameter binding nodes.

        Nodes bind function parameters to locations in config or state
        dictionaries. Subclasses :class:`Config` and :class:`State` implement
        specific binding behaviors.

        Attributes:
            Param: namedtuple with fields (name, type, default, empty).
            Binding: Class with attributes (node, root, root_name).
        """

        Param = namedtuple("Param", ["name", "type", "default", "empty"])

        class Binding:
            node: str
            root: dict
            root_name: str

        def __init__(self, node):
            """Initialize node with path.

            Args:
                node: Dot-separated path to node (e.g., "elasticsearch.url").
            """
            self.node = node

        @abstractmethod
        def handle_param(self, param, config, state, core_logger):
            """Create binding for parameter.

            Args:
                param: Param namedtuple describing the parameter.
                config: Configuration dictionary.
                state: State dictionary.
                core_logger: Logger for core operations.

            Returns:
                Tuple of (binding, getter, setter) functions.
            """

    class Config(Node):
        """Bind parameter to configuration node.

        Looks up values in the pipe's configuration dictionary. Supports
        indirect references via "@" suffix, which redirects lookup to a
        state node.

        Example::

            # Direct config reference
            threshold: Annotated[int, Pipe.Config("threshold")]

            # Indirect reference: config specifies which state node to use
            # Config: {"data@": "input.documents"}
            # State: {"input": {"documents": [...]}}
            data: Annotated[list, Pipe.Config("data")]

        Args:
            node: Dot-separated path in configuration.
            indirect: Whether to support an indirect config node via an "@"
                suffix, or which node name to use for that indirection.
        """

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
        """Bind parameter to state node.

        Looks up values in the shared state dictionary. By default, supports
        indirect references via config nodes ending with "@".

        Example::

            # Scalar state reference (read-only; no mutable needed)
            threshold: Annotated[int, Pipe.State("threshold")]

            # List/dict values require mutable=True, even just to read
            # (see 'mutable' below)
            docs: Annotated[list, Pipe.State("documents", mutable=True)]

            # Mutable access: the object can be modified in place
            results: Annotated[list, Pipe.State("output", mutable=True)]

            # Whole state (node=None)
            state: Annotated[dict, Pipe.State(None, mutable=True)]

            # Custom indirect config key: override via "source@", not "default.path@"
            data: Annotated[list, Pipe.State("default.path", indirect="source", mutable=True)]

        Args:
            node: Dot-separated path in state, or None for whole state.
            indirect: Whether to support a config override via an "@" suffix,
                which node name to use for that override, or False to
                disable.
            mutable: If True, allow receiving a mutable (list/dict) value.
                Required even for read-only access to list/dict values, since
                once a pipe holds such a reference, in-place modification can
                be neither detected nor prevented.
                Inside a :class:`Pipe.Context`, mutable=True also lets the
                attribute be reassigned (``ctx.name = value``), writing a new
                value back to state via the generated property setter; plain
                pipe function parameters have no such write-back mechanism.
        """

        def __init__(self, node, *, indirect=True, mutable=False):
            super().__init__(node)
            self.indirect = indirect
            self.mutable = mutable
            if node is None and not isinstance(indirect, str):
                self.indirect = False
            if node is not None and node.startswith("runtime."):
                # Indirection may redirect a lookup into runtime.*; a parameter
                # already bound there is already at that destination.
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
    """Context for common configuration parameters.

    Provides logging configuration shared across pipes. Automatically
    included in all pipe executions.

    Attributes:
        logging_level: Log level string (debug, info, warning, error, critical).
    """

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
    logging_format: Annotated[
        str,
        Pipe.Config("logging.format"),
    ] = "text",
    dry_run: bool = False,
):
    """Core pipe for version checking, search path, and logging format configuration.

    Args:
        min_version: Minimum required elastic-pipes version (semver format).
        search_path: Additional paths to search for pipe modules.
        logging_format: Log line format, "text" or "json" (ECS-compliant).
        dry_run: Accepted but unused; version checking and search path setup
            always run regardless of dry-run.

    Raises:
        ConfigError: If current version is below minimum required version,
            or logging_format is not a recognized format name.
    """
    # applied first: any error raised below should already be logged in the chosen format
    if not getattr(logging.getLogger("elastic.pipes"), "format_overridden", False):
        from .util import set_logging_format

        set_logging_format(logging_format)

    if min_version is not None:
        from semver import VersionInfo

        if VersionInfo.parse(__version__) < VersionInfo.parse(min_version):
            raise ConfigError(f"current version is older than minimum version: {__version__} < {min_version}")
