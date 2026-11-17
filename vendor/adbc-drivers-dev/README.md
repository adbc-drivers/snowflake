<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Vendored `adbc-drivers-dev`

Source: <https://github.com/adbc-drivers/dev>

Exact upstream revision: `0c94e3235eee46cc3ace6aa63daa203f3abcf53d`

License: Apache-2.0; the upstream license and notice are included as `LICENSE.txt` and `NOTICE.txt`.

Included scope: the complete Git-tracked source tree at that revision, including `.github/`, `adbc_drivers_dev/`, `docs/`, `schema/`, `tests/`, build metadata, and `pixi.lock`. The upstream root `README.md` is replaced by this vendor-specific README. Git metadata and untracked or ignored artifacts (such as caches, `__pycache__/`, and `*.egg-info/`) are not included. Upstream `.env` pins Go 1.27.3 and Rust 1.99.0.

Local modifications:

1. `adbc_drivers_dev/make_config.py` sets writable Go (`GOCACHE=/tmp/go-build`, `GOMODCACHE=/tmp/go-mod`, `GOPATH=/tmp/go`) and Rust (`HOME=/tmp`, `XDG_CACHE_HOME=/tmp/.cache`, `CARGO_HOME=/.cargo`) cache locations in the inner manylinux `docker exec` build commands, which run under arbitrary host UIDs.
2. `adbc_drivers_dev/compose/manylinux-rust/Dockerfile` installs `perl-IPC-Cmd` and `perl-Time-Piece` for vendored OpenSSL builds; `adbc_drivers_dev/compose.yaml` gives this changed Rust image a `-snowflake2` suffix to prevent pulling an upstream image without those modules, and omits the service's multi-platform `platforms` list so the unpublished image builds for `DOCKER_DEFAULT_PLATFORM` with the runners' default docker driver.
3. `adbc_drivers_dev/templates/test.yaml` serializes Snowflake live validation jobs across Go and Rust with `snowflake-live-validation` and `cancel-in-progress: false`, because they share fixed table names. Snowflake Rust test, validation, and build matrices keep `fail-fast: false` and disable incremental compilation to limit CI cache pressure; Rust lint runs `cargo fmt --check` before Clippy.
4. `adbc_drivers_dev/templates/pixi.toml` keeps Snowflake Go and Rust on the editable `../vendor/adbc-drivers-dev` package when generated, rather than resetting to upstream Git; other drivers retain the upstream Git dependency.
5. `tests/test_make.py` asserts the Go and Rust cache variables appear in the actual inner `docker exec` command rather than only the outer Compose environment; `tests/test_workflow.py` checks the generated Pixi dependency source for Snowflake Go/Rust and other drivers.

6. `adbc_drivers_dev/templates/test.yaml` extends Snowflake Rust unit-test and package-test matrices to Linux ARM64 and Windows ARM64, limits non-release test matrices to three concurrent jobs, and selects native ARM64 `dumpbin` on Windows ARM64. Upstream already supplies Windows ARM64 builds and package tests; its unit-test matrix and Linux ARM64 package-test coverage remain narrower.
7. `adbc_drivers_dev/package.py` includes `aarch64-pc-windows-msvc` in the cargo-about license targets so the Windows ARM64 package attributes crates linked only on that target (for example `windows_aarch64_msvc`).

Regenerate with `adbc-gen-workflow generate <repository>` from an environment using this vendored package. Keep the generated Rust workflows and `rust/pixi.toml`; do not replace the Go workflows as part of a Rust-only refresh. The Snowflake `LOCAL MODIFICATIONS` blocks live in the templates and survive regeneration. Refresh `rust/pixi.lock` with `pixi lock` from `rust/` after regenerating its manifest.
