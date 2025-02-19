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

"""Elastic Pipes component to rewrite the timestamp of documents."""

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from logging import Logger

from typing_extensions import Annotated

from . import Pipe
from .errors import ConfigError
from .util import parse_timestamp


class StrategyNow:
    """Use time at the current moment of the rewrite."""

    def __init__(self):
        pass

    def __call__(self, ts):
        return datetime.now(timezone.utc)


class StrategyNowFirst:
    """Use time at the current moment of the rewrite only for the first document,
    all the others keep the same delta from the first."""

    def __init__(self):
        self.start = datetime.now(timezone.utc)
        self.first = None

    def __call__(self, ts):
        if self.first is None:
            self.first = ts
        delta = ts - self.first
        return self.start + delta


class Ctx(Pipe.Context):
    strategies = {
        "now": StrategyNow,
        "now-first": StrategyNowFirst,
    }

    docs: Annotated[
        Iterable,
        Pipe.State("documents", mutable=True),
    ] = None
    ts_field: Annotated[
        str,
        Pipe.Config("timestamp-field"),
        Pipe.Help("name of the timestamp field to be rewritten"),
    ] = "@timestamp"
    strategy_name: Annotated[
        str,
        Pipe.Config("strategy-name"),
        Pipe.Help(f"one among: {', '.join(sorted(strategies))}"),
    ] = "now"
    strategy_params: Annotated[
        Mapping,
        Pipe.Config("strategy-params"),
        Pipe.Help("strategy specific parameters"),
    ] = None

    def __init__(self):
        if self.strategy_name not in self.strategies:
            raise ConfigError(f"unknown strategy: {self.strategy_name} (allowed strategies: {', '.join(sorted(self.strategies))})")
        params = self.strategy_params or {}
        self.strategy = self.strategies[self.strategy_name](**params)


@Pipe("elastic.pipes.core.timestamp-rewrite", default={})
def main(
    pipe: Pipe,
    ctx: Ctx,
    log: Logger,
):
    """Rewrite the timestamp of the input documents according to some strategy."""

    if not ctx.docs:
        return

    def _rewrite(docs):
        log.info(f"rewriting the '{ctx.ts_field}' field with strategy '{ctx.strategy_name}'")

        for doc in docs:
            ts = datetime.now(timezone.utc)
            try:
                if ctx.ts_field in doc:
                    ts = parse_timestamp(doc[ctx.ts_field])
            except Exception as e:
                raise ValueError(f"'{ctx.ts_field}' parse error: {e}")

            doc[ctx.ts_field] = ctx.strategy(ts).isoformat(timespec="milliseconds")
            yield doc

    if isinstance(ctx.docs, Sequence):
        ctx.docs = list(_rewrite(ctx.docs))
    else:
        ctx.docs = _rewrite(ctx.docs)


if __name__ == "__main__":
    main()
