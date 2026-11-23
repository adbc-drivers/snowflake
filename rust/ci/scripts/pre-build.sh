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
    if [[ "$3" == "arm64" ]]; then
        # AWS-LC requires clang-cl, not NASM, for aarch64-pc-windows-msvc:
        # https://aws.github.io/aws-lc-rs/requirements/windows.html
        # The Windows ARM runner image includes LLVM and the MSVC ARM64 tools.
        # aws-lc-sys and ring select clang-cl/clang themselves once LLVM is on
        # PATH, so no CC_* overrides (which only the build step would see).
        tool_dir="${ProgramFiles:-C:\\Program Files}\\LLVM\\bin"
        if [[ ! -f "$(cygpath -u "$tool_dir")/clang-cl.exe" ]]; then
            echo "LLVM clang-cl was not found in $tool_dir" >&2
            exit 1
        fi
    else
        choco install nasm -y --no-progress
        tool_dir="${ProgramFiles:-C:\\Program Files}\\NASM"
        if [[ ! -f "$(cygpath -u "$tool_dir")/nasm.exe" ]]; then
            echo "NASM was not installed in $tool_dir" >&2
            exit 1
        fi
    fi
    printf '%s\n' "$tool_dir" >> "$GITHUB_PATH"
    # GITHUB_PATH takes effect next step; the caller also builds in this step.
    printf 'export PATH=%q:$PATH\n' "$(cygpath -u "$tool_dir")" >> .env.build
fi
