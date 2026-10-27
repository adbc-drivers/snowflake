#!/usr/bin/env bash
# Copyright (c) 2026 ADBC Drivers Contributors
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

set -ex

# The vendored manylinux image already includes the OpenSSL dependencies and
# writable caches. Avoid changing it here: release builds require a clean tree.
if [[ "$2" == "windows" ]]; then
    choco install nasm -y --no-progress
    nasm_dir="${ProgramFiles:-C:\\Program Files}\\NASM"
    if [[ ! -f "$(cygpath -u "$nasm_dir")/nasm.exe" ]]; then
        echo "NASM was not installed in $nasm_dir" >&2
        exit 1
    fi
    printf '%s\n' "$nasm_dir" >> "$GITHUB_PATH"
fi
