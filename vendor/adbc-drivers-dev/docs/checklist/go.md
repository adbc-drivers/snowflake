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

# Upgrading Go version

- [ ] Bump `GO` in `adbc_drivers_dev/.env`
- [ ] Trigger [Docker Compose Build][docker-compose-build-workflow] workflow
      with "Push to ghcr.io"
- [ ] In driverbase and downstream drivers:
  - [ ] Update `go` declaration in go.mod files
  - [ ] Update adbc-dev dependency (if updating a driver)

    ```shell
    pixi add --pypi 'adbc-drivers-dev @ git+https://github.com/adbc-drivers/dev'
    ```

    You may need to blow away the `pixi.lock` first.

  - [ ] Update driverbase dependency (if updating a driver)

    ```shell
    go get -u github.com/adbc-drivers/driverbase-go/{driverbase,testutil,validation}
    ```

    This will automatically update the `go` declaration if needed.

  - [ ] Update golangci-lint

    ```shell
    pre-commit autoupdate --freeze --repo https://github.com/golangci/golangci-lint
    ```

  - [ ] Purge the GitHub Actions cache (as otherwise CI will use the previously built version of golangci-lint)

    ```shell
    gh cache list -k pre-commit --json 'key' | jq -r '.[] | .key' | xargs -n1 gh cache delete
    ```

[docker-compose-build-workflow]: https://github.com/adbc-drivers/dev/actions/workflows/docker-build.yaml
