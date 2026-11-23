# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import tomlkit

import adbc_drivers_dev.package as package


@pytest.mark.parametrize(
    "name,expected",
    [
        ("libadbc_driver_foo.dll", "foo"),
        ("libadbc_driver_foo.so", "foo"),
        ("libadbc_driver_foo.dylib", "foo"),
        ("libadbc_driver_foo-bar.dll", "foo-bar"),
        ("adbc_driver_foo-bar.dll", "foo-bar"),
        ("adbc-driver-foo-bar.dll", None),
        ("libcolumnar_driver_foo-bar.dll", None),
        ("libadbc_driverz_foo-bar.dll", None),
        ("libadbc_foo-bar.dll", None),
    ],
)
def test_normalize_driver_name(name: str, expected: str | None) -> None:
    if expected is None:
        with pytest.raises(ValueError):
            package.normalize_driver_name(name)
    else:
        assert package.normalize_driver_name(name) == expected


def test_validate_manifest() -> None:
    manifest = tomlkit.TOMLDocument({})
    with pytest.raises(ValueError, match="missing required `name`"):
        package.validate_manifest(manifest)

    manifest["name"] = "example"
    with pytest.raises(ValueError, match="missing required `description`"):
        package.validate_manifest(manifest)

    manifest["description"] = "An example package"
    with pytest.raises(ValueError, match="missing required `publisher`"):
        package.validate_manifest(manifest)

    manifest["publisher"] = "Example Publisher"
    with pytest.raises(ValueError, match="missing required `license`"):
        package.validate_manifest(manifest)

    manifest["license"] = "Apache-2.0"
    with pytest.raises(ValueError, match="missing required `version`"):
        package.validate_manifest(manifest)

    manifest["version"] = "1.0.0"
    with pytest.raises(ValueError, match="missing required `Files`"):
        package.validate_manifest(manifest)

    manifest["Files"] = {}
    with pytest.raises(ValueError, match="missing required `Files.driver`"):
        package.validate_manifest(manifest)

    manifest["Files"]["driver"] = "libexample.so"
    package.validate_manifest(manifest)
