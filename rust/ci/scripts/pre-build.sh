#!/usr/bin/env bash

set -ex

if [[ "$2" == "windows" ]]; then
    choco install openssl -y
    echo "OPENSSL_DIR='C:\Program Files\OpenSSL'" > .env.build
fi
