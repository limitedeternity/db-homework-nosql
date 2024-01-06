#!/usr/bin/env bash

CODEGEN_REPO="limitedeternity/cmake_codegen"
CODEGEN_COMMIT="c34fdf0f6899b7475adeb890a946fe58019649e0"
CODEGEN_DIR="_codegen"

if ! [[ -d "$CODEGEN_DIR" ]]; then
    TAR_URL="https://github.com/$CODEGEN_REPO/tarball/$CODEGEN_COMMIT"
    echo "Downloading $TAR_URL..."

    curl -sSL "$TAR_URL" -o repo.tar.gz
    tar -xzf repo.tar.gz --strip=1 --wildcards "$(echo "$CODEGEN_REPO" | awk '{gsub(/\//,"-"); print}')-*/$CODEGEN_DIR"
    rm repo.tar.gz
fi
