#!/usr/bin/env bash

set -ex

if [[ "$2" == "windows" ]]; then
    choco install openssl -y
    echo "OPENSSL_DIR='C:\Program Files\OpenSSL'" > .env.build
elif [[ "$2" == "linux" ]]; then
    echo "Patching adbc-drivers-dev manylinux-rust image to include perl modules for openssl vendored build"
    
    pixi install
    
    # Find adbc_drivers_dev directory via pixi
    ADBC_DEV_DIR=$(pixi run python -c "import os, adbc_drivers_dev; print(os.path.dirname(adbc_drivers_dev.__file__))" 2>/dev/null || true)
    
    if [ -n "$ADBC_DEV_DIR" ] && [ -f "$ADBC_DEV_DIR/.env" ]; then
        MANYLINUX=$(grep -oP '(?<=^MANYLINUX=).*' "$ADBC_DEV_DIR/.env" || true)
        RUST=$(grep -oP '(?<=^RUST=).*' "$ADBC_DEV_DIR/.env" || true)
        
        if [ -n "$MANYLINUX" ] && [ -n "$RUST" ]; then
            IMAGE="ghcr.io/adbc-drivers/dev:${MANYLINUX}-rust${RUST}"
            echo "Patching Docker image: $IMAGE"
            
            docker pull "$IMAGE" || true
            CONTAINER_ID=$(docker run -d -u root "$IMAGE" bash -c "yum install -y perl-IPC-Cmd perl-Time-Piece && yum clean all")
            EXIT_CODE=$(docker wait "$CONTAINER_ID")
            if [ "$EXIT_CODE" -ne 0 ]; then
                echo "Failed to install perl modules in $IMAGE"
                docker logs "$CONTAINER_ID"
                exit 1
            fi
            docker commit "$CONTAINER_ID" "$IMAGE"
            docker rm "$CONTAINER_ID"
        else
            echo "Failed to extract MANYLINUX or RUST from $ADBC_DEV_DIR/.env"
            exit 1
        fi
    else
        echo "Could not find adbc_drivers_dev via pixi. Skipping patch."
        exit 1
    fi
fi
