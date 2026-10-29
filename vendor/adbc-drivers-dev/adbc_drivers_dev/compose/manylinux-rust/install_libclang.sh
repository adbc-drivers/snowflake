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

# This is now owned by the LLVM project, but they appear to be a bit behind on
# releases.  However for our purposes 18.1.1 is fine.
# https://github.com/sighingnow/libclang/issues/84

set -euo pipefail

main() {
    local -r clang="${1}"
    local -r platform="${2}"

    if [[ "$platform" == "linux/amd64" ]]; then
        local -r url="https://files.pythonhosted.org/packages/1d/fc/716c1e62e512ef1c160e7984a73a5fc7df45166f2ff3f254e71c58076f7c/libclang-18.1.1-py2.py3-none-manylinux2010_x86_64.whl"
    elif [[ "$platform" == "linux/arm64" ]]; then
        local -r url="https://files.pythonhosted.org/packages/3c/3d/f0ac1150280d8d20d059608cf2d5ff61b7c3b7f7bcf9c0f425ab92df769a/libclang-18.1.1-py2.py3-none-manylinux2014_aarch64.whl"
    else
        echo "Unsupported platform: $arch"
        exit 1
    fi

    wget --no-verbose -O libclang.whl "${url}"
    unzip -d /opt/libclang libclang.whl
    rm libclang.whl
}

main "$@"
