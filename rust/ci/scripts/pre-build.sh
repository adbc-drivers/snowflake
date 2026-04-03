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

if [[ "$2" == "windows" ]]; then
    choco install openssl -y
    echo "OPENSSL_DIR='C:\Program Files\OpenSSL'" > .env.build
elif [[ "$2" == "linux" ]]; then
    echo "Checking if we need to patch adbc-drivers-dev manylinux-rust Dockerfile"

    # Ensure pixi environment is set up so we can find adbc_drivers_dev
    pixi install

    ADBC_DEV_DIR=$(pixi run python -c "import os, adbc_drivers_dev; print(os.path.dirname(adbc_drivers_dev.__file__))")
    DOCKERFILE="$ADBC_DEV_DIR/compose/manylinux-rust/Dockerfile"
    COMPOSEFILE="$ADBC_DEV_DIR/compose.yaml"

    if [ -f "$DOCKERFILE" ] && [ -f "$COMPOSEFILE" ]; then
        echo "Patching $DOCKERFILE to include perl modules for openssl vendored build"
        sed -i 's/wget openssl openssl-devel openssl-static/wget openssl openssl-devel openssl-static perl-IPC-Cmd perl-Time-Piece/g' "$DOCKERFILE"

        echo "Patching $COMPOSEFILE to use local image tag and set HOME=/tmp"
        sed -i 's/image: ghcr.io\/adbc-drivers\/dev/image: local-patched\/adbc-drivers-dev/g' "$COMPOSEFILE"
        grep -q 'HOME=/tmp' "$COMPOSEFILE" || \
            sed -i 's/^    volumes:/    environment:\n      - HOME=\/tmp\n    volumes:/' "$COMPOSEFILE"
    else
        echo "Could not find Dockerfile at $DOCKERFILE or compose.yaml at $COMPOSEFILE — aborting."
        exit 1
    fi
fi
