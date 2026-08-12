# Copyright (c) 2026 ADBC Drivers Contributors
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

# LOCAL MODIFICATIONS: Snowflake adds deterministic regressions for the vendored
# suffix and arbitrary-UID manylinux cache fixes.

import subprocess

import pytest
from adbc_drivers_dev.make import detect_version, maybe_build_docker


def test_manylinux_build_uses_writable_go_caches(tmp_path, monkeypatch):
    from adbc_drivers_dev import make

    calls = []
    monkeypatch.setattr(
        make, "check_call", lambda *args, **kwargs: calls.append((args, kwargs))
    )
    monkeypatch.setattr(make, "get_var", lambda name, default: default)
    monkeypatch.setattr(make.platform, "system", lambda: "Linux")

    maybe_build_docker(
        repo_root=tmp_path,
        driver_root=tmp_path,
        env={},
        args=["go", "build", "./pkg"],
        ci=True,
        container="manylinux",
    )

    command = calls[0][0][0]
    service_index = command.index("manylinux")
    assert command[service_index - 6 : service_index] == [
        "--env",
        "GOCACHE=/tmp/go-build",
        "--env",
        "GOMODCACHE=/tmp/go-mod",
        "--env",
        "GOPATH=/tmp/go",
    ]


@pytest.fixture()
def repo(tmp_path):
    subprocess.check_call(["git", "init", "--quiet"], cwd=tmp_path)
    subprocess.check_call(["git", "config", "user.name", "Test User"], cwd=tmp_path)
    subprocess.check_call(
        ["git", "config", "user.email", "test@example.com"], cwd=tmp_path
    )
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "chore: initial commit"],
        cwd=tmp_path,
    )
    return tmp_path


def test_no_tags(repo):
    version = detect_version(repo)
    assert version == "v0.0.1-dev"


def test_no_tags_strict(repo):
    with pytest.raises(ValueError, match="No tags found"):
        detect_version(repo, strict=True)


def test_on_tag(repo):
    subprocess.check_call(["git", "tag", "v1.2.3"], cwd=repo)
    version = detect_version(repo)
    assert version == "v1.2.3"

    version = detect_version(repo, strict=True)
    assert version == "v1.2.3"


def test_on_tag_subdir(repo):
    subprocess.check_call(["git", "tag", "rust/v1.2.3"], cwd=repo)
    version = detect_version(repo / "rust")
    assert version == "v1.2.3"

    version = detect_version(repo / "rust", strict=True)
    assert version == "v1.2.3"


def test_after_tag(repo):
    subprocess.check_call(["git", "tag", "v1.2.3"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    version = detect_version(repo)
    assert version.startswith("v1.2.3-dev.1."), version


def test_after_tag_uses_semver_revision_prefix(repo, monkeypatch):
    subprocess.check_call(["git", "tag", "v1.2.3"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )

    from adbc_drivers_dev import make

    original_check_output = make.check_output

    def check_output(args, *posargs, **kwargs):
        if args == ["git", "rev-parse", "--short", "HEAD"]:
            return "0747374"
        return original_check_output(args, *posargs, **kwargs)

    monkeypatch.setattr(make, "check_output", check_output)
    assert detect_version(repo) == "v1.2.3-dev.1.g0747374"


def test_after_tag_subdir(repo):
    subprocess.check_call(["git", "tag", "rust/v1.2.3"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    version = detect_version(repo / "rust")
    assert version.startswith("v1.2.3-dev.1."), version


def test_after_prerelease(repo):
    subprocess.check_call(["git", "tag", "v1.2.3-alpha.1"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    version = detect_version(repo)
    # This is a weird edge case
    assert version.startswith("v0.0.1-dev.2."), version


def test_after_prerelease2(repo):
    subprocess.check_call(["git", "tag", "v1.2.3"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    subprocess.check_call(["git", "tag", "v1.3.0-alpha.1"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    version = detect_version(repo)
    assert version.startswith("v1.2.3-dev.2."), version


def test_after_prerelease_on_tag(repo):
    subprocess.check_call(["git", "tag", "v1.2.3-alpha.1"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    subprocess.check_call(["git", "tag", "v1.2.3"], cwd=repo)
    version = detect_version(repo)
    assert version == "v1.2.3"


def test_versions_are_inverted(repo):
    # detect when versions are out of order
    subprocess.check_call(["git", "tag", "v1.2.3"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    subprocess.check_call(["git", "tag", "v1.2.3-alpha.1"], cwd=repo)

    with pytest.raises(
        ValueError,
        match="Tag v1.2.3 is further from HEAD than v1.2.3-alpha.1, but has a newer version",
    ):
        detect_version(repo)


def test_strict_not_on_tag(repo):
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    subprocess.check_call(["git", "tag", "v1.0.0"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    subprocess.check_call(["git", "tag", "v1.2.3"], cwd=repo)
    subprocess.check_call(
        ["git", "commit", "--quiet", "--allow-empty", "-m", "feat: add new feature"],
        cwd=repo,
    )
    with pytest.raises(
        ValueError, match="is not on tag v1.2.3, but has 1 commits since"
    ):
        detect_version(repo, strict=True)


def test_strict_dirty(repo):
    subprocess.check_call(["git", "tag", "v1.2.3"], cwd=repo)
    with (repo / "foobar.txt").open("w"):
        pass
    subprocess.check_call(["git", "add", "foobar.txt"], cwd=repo)
    with pytest.raises(ValueError, match="has uncommitted changes"):
        detect_version(repo, strict=True)
