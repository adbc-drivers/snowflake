#!/bin/bash
# Copyright (c) 2026 ADBC Drivers Contributors
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

main() {
    # Rust always exports no_mangle symbols from a cdylib. On macOS, rustc
    # passes them to the linker in an exported symbols list, which includes
    # C APIs of dependencies (crc-fast, sf_core) that this driver must not
    # export. Keep only the ADBC entry points in the driver dylib; other links
    # (proc macros, tests) need their lists intact. Linux uses --exclude-libs.
    # https://github.com/rust-lang/rust/issues/73958
    local output="" previous_arg=""
    for arg in "$@"; do
        if [[ "${previous_arg}" == -o ]]; then
            output="${arg}"
        fi
        previous_arg="${arg}"
    done

    if [[ "$(basename "${output}")" == libadbc_driver_snowflake*.dylib ]]; then
        previous_arg=""
        for arg in "$@"; do
            local exported_symbols_list=""
            if [[ "${previous_arg}" == -Wl,-exported_symbols_list && "${arg}" == -Wl,* ]]; then
                exported_symbols_list="${arg#-Wl,}"
            elif [[ "${arg}" == -Wl,-exported_symbols_list,* ]]; then
                exported_symbols_list="${arg#-Wl,-exported_symbols_list,}"
            fi

            if [[ -n "${exported_symbols_list}" ]]; then
                local scratch
                scratch=$(mktemp)
                grep '^_Adbc' "${exported_symbols_list}" > "${scratch}" || true
                mv "${scratch}" "${exported_symbols_list}"
            fi

            previous_arg="${arg}"
        done
    fi
    cc "$@"
}

main "$@"
