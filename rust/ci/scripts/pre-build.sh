#!/usr/bin/env bash

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
        # Change the image tag so docker compose doesn't try to pull it from ghcr.io and instead builds it locally
        sed -i 's/image: ghcr.io\/adbc-drivers\/dev/image: local-patched\/adbc-drivers-dev/g' "$COMPOSEFILE"
        # Inject HOME=/tmp so that rust crates (like protoc) can write to cache directories when running as non-root user
        sed -i 's/^    volumes:/    environment:\n      - HOME=\/tmp\n    volumes:/' "$COMPOSEFILE"
    fi
fi
