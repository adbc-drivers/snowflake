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
    local -r rust="${1}"
    local -r platform="${2}"

    if [[ "$platform" == "linux/amd64" ]]; then
        export ARCH="x86_64"
    elif [[ "$platform" == "linux/arm64" ]]; then
        export ARCH="aarch64"
    else
        echo "Unsupported platform: $arch"
        exit 1
    fi

    wget --no-verbose https://static.rust-lang.org/dist/rust-${RUST}-${ARCH}-unknown-linux-gnu.tar.xz
    mkdir -p /tmp/rust
    tar --strip-components=1 -C /tmp/rust -xf rust-${RUST}-${ARCH}-unknown-linux-gnu.tar.xz
    rm rust-${RUST}-${ARCH}-unknown-linux-gnu.tar.xz
    /tmp/rust/install.sh
    rm -rf /tmp/rust
    rustc -V
}

main "$@"
