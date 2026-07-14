#!/usr/bin/env bash
set -euo pipefail

if rg -n '^\[patch\.' --glob 'Cargo.toml' --glob '**/Cargo.toml' .; then
  echo 'external Cargo patches are forbidden' >&2
  exit 1
fi

if rg -n 'path\s*=\s*"\.\./' --glob 'Cargo.toml' --glob '**/Cargo.toml' .; then
  echo 'Cargo paths outside this repository are forbidden' >&2
  exit 1
fi

if rg -n 'mutsuki-service-(runtime|host)' --glob 'Cargo.toml' --glob '**/Cargo.toml' .; then
  echo 'DistributedHost may consume ServiceHost control/ipc crates, not its runtime' >&2
  exit 1
fi

echo 'distributed ownership boundaries passed'
