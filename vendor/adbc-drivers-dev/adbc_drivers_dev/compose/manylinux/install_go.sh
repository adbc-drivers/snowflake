#!/bin/bash
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

main() {
    local -r go="${1}"
    local -r platform="${2}"

    if [[ "$platform" == "linux/amd64" ]]; then
        export ARCH="amd64"
    elif [[ "$platform" == "linux/arm64" ]]; then
        export ARCH="arm64"
    else
        echo "Unsupported platform: $arch"
        exit 1
    fi

    wget --no-verbose https://go.dev/dl/go${go}.linux-${ARCH}.tar.gz
    tar -C /usr/local -xzf go${go}.linux-${ARCH}.tar.gz
    rm go${go}.linux-${ARCH}.tar.gz
}

main "$@"
