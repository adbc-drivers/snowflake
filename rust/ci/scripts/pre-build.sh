#!/usr/bin/env bash

set -ex

if [[ "$2" == "windows" ]]; then
    echo "OPENSSL_DIR='C:\Program Files\OpenSSL-Win64'" > .env.build
fi
