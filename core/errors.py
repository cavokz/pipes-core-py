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

"""Exception classes for Elastic Pipes."""


class Error(RuntimeError):
    """Base exception for Elastic Pipes errors.

    Raised for runtime errors during pipe execution, such as missing
    required parameters, type mismatches, or parameter access violations.
    """


class ConfigError(Error):
    """Configuration or state structure error.

    Raised when:

    - State or config structure is invalid (not a mapping, wrong types)
    - Pipe configuration contains unknown nodes
    - Required version check fails
    - Pipe names conflict or are malformed
    - Interactive mode is attempted without explicit permission
    """


class ShellExpansionError(Error):
    """Shell expansion operation failed.

    Raised when shell-expand is enabled and the expansion command
    fails or returns invalid data.
    """
