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

Exact upstream revision: `eea5591d7d1c7fe0047c7d1a1109afe1be50e4b4`

License: Apache-2.0; the upstream license and notice are included as `LICENSE.txt` and `NOTICE.txt`.

Included scope: the complete installable `adbc_drivers_dev` package, `pyproject.toml`, `MANIFEST.in`, `LICENSE.txt`, `NOTICE.txt`, and the complete upstream `tests/test_detect_version.py` module from the revision above.

Local modifications (exactly two):

1. `adbc_drivers_dev/make.py` prefixes the development-version git revision with `g`, producing SemVer-compatible versions such as `v1.2.3-dev.1.g0747374`.
2. `tests/test_detect_version.py` adds one deterministic regression test for that `g` prefix.
