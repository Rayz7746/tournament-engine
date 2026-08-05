#!/usr/bin/env bash

set -euo pipefail

readonly PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v protoc >/dev/null 2>&1; then
    echo "protoc is required. Install it on Arch Linux with:" >&2
    echo "  sudo pacman -S protobuf" >&2
    exit 1
fi

if ! command -v protoc-gen-go >/dev/null 2>&1; then
    echo "protoc-gen-go is required. Install it with:" >&2
    echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11" >&2
    exit 1
fi

if ! command -v protoc-gen-go-grpc >/dev/null 2>&1; then
    echo "protoc-gen-go-grpc is required. Install it with:" >&2
    echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2" >&2
    exit 1
fi

cd -- "${PROJECT_ROOT}"

protoc \
    --go_out=. \
    --go_opt=module=tournament-engine \
    --go-grpc_out=. \
    --go-grpc_opt=module=tournament-engine \
    proto/pairing/v1/pairing.proto
