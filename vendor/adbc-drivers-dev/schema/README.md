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

# ./schema

JSON schema for the `generate.toml`. `generate-schema.json` is automatically
generated and shouldn't be edited by hand. See below.

## How to Update

1. Edit `workflow.py` as needed
2. Run `adbc-gen-workflow generate-schema` to sync `generate-schema.json`

## How to Use

Ensure your `generate.toml` has the following at the top:

```toml
#:schema https://raw.githubusercontent.com/adbc-drivers/dev/refs/heads/main/schema/generate-schema.json"

```

Then, if you have the [tombi](https://tombi-toml.github.io/) language server set up, you should automatically get in-editor validation and documentation.

You can also lint your generate.toml with,

```sh
tombi lint .github/workflows/generate.toml
```

if you prefer.
