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

import functools
import os
import platform
import shlex
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

from adbc_drivers_dev import make_config
from adbc_drivers_dev.make_config import (
    LangGo,
    LangRust,
    LangScript,
    MakeConfig,
    MakeEnv,
    merge_build_env,
)


@pytest.fixture(scope="session")
def rust_driver_root() -> tuple[Path, Path]:
    repo_root = Path(__file__).parent.parent
    rust_driver_root = repo_root / "tests" / "make" / "rustdummy"
    return repo_root, rust_driver_root


@pytest.fixture(scope="session")
def go_driver_root() -> tuple[Path, Path]:
    repo_root = Path(__file__).parent.parent
    go_driver_root = repo_root / "tests" / "make" / "godummy"
    return repo_root, go_driver_root


def debug_subprocess(func: callable) -> callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except subprocess.CalledProcessError as e:
            print(
                ">>> STDOUT >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", file=sys.stderr
            )
            print(e.stdout, file=sys.stderr)
            print(
                ">>> STDERR >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", file=sys.stderr
            )
            print(e.stderr, file=sys.stderr)
            raise

    return wrapper


def test_build_config(rust_driver_root: tuple[Path, Path]) -> None:
    repo_root, driver_root = rust_driver_root
    config = MakeEnv(
        ci=False,
        debug=False,
        host_platform="linux",
        host_architecture="amd64",
        target_platform="linux",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    assert config.shared_library_affix == ("lib", ".so")
    assert not config.use_docker

    config = MakeEnv(
        ci=False,
        debug=True,
        host_platform="linux",
        host_architecture="amd64",
        target_platform="linux",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    assert not config.use_docker

    config = MakeEnv(
        ci=False,
        debug=True,
        host_platform="macos",
        host_architecture="arm64",
        target_platform="linux",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    assert config.use_docker

    config = MakeEnv(
        ci=False,
        debug=True,
        host_platform="macos",
        host_architecture="arm64",
        target_platform="macos",
        target_architecture="arm64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    assert not config.use_docker

    config = MakeEnv(
        ci=True,
        debug=True,
        host_platform="macos",
        host_architecture="arm64",
        target_platform="macos",
        target_architecture="arm64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    assert not config.use_docker

    config = MakeEnv(
        ci=True,
        debug=False,
        host_platform="linux",
        host_architecture="amd64",
        target_platform="linux",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    assert config.use_docker


def test_merge_build_env() -> None:
    assert merge_build_env(
        {"CGO_CFLAGS": "-O2", "OTHER": "old"},
        {"CGO_CFLAGS": "-mmacos-version-min=13.0", "OTHER": "new"},
    ) == {
        "CGO_CFLAGS": "-O2 -mmacos-version-min=13.0",
        "OTHER": "new",
    }
    assert merge_build_env(
        {"CGO_LDFLAGS": ""}, {"CGO_LDFLAGS": "-mmacos-version-min=13.0"}
    ) == {"CGO_LDFLAGS": "-mmacos-version-min=13.0"}


def test_manylinux_config_overrides_environment(
    monkeypatch: pytest.MonkeyPatch, rust_driver_root: tuple[Path, Path]
) -> None:
    repo_root, driver_root = rust_driver_root
    monkeypatch.setenv("MANYLINUX", "manylinux2014")
    config = MakeEnv(
        ci=True,
        debug=False,
        host_platform="linux",
        host_architecture="amd64",
        target_platform="linux",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    make_config = MakeConfig(
        driver="rustdummy",
        lang=LangRust(lang="rust"),
        manylinux="manylinux_2_28",
    )

    plan = make_config.build_plan(config)

    assert plan._docker_outer_env()["MANYLINUX"] == "manylinux_2_28"


# LOCAL MODIFICATIONS: assert writable manylinux caches reach the inner docker exec.
@pytest.mark.parametrize("language", ["go", "rust"])
def test_manylinux_exec_uses_writable_caches(
    language: str,
    monkeypatch: pytest.MonkeyPatch,
    go_driver_root: tuple[Path, Path],
    rust_driver_root: tuple[Path, Path],
) -> None:
    if language == "go":
        repo_root, driver_root = go_driver_root
        lang = LangGo(lang="go", go_mod_path="module")
        container = "manylinux"
        cache_env = {
            "GOCACHE": "/tmp/go-build",
            "GOMODCACHE": "/tmp/go-mod",
            "GOPATH": "/tmp/go",
        }
    else:
        repo_root, driver_root = rust_driver_root
        lang = LangRust(lang="rust")
        container = "manylinux-rust"
        cache_env = {
            "HOME": "/tmp",
            "XDG_CACHE_HOME": "/tmp/.cache",
            "CARGO_HOME": "/.cargo",
        }

    make_env = MakeEnv(
        ci=True,
        debug=False,
        host_platform="linux",
        host_architecture="amd64",
        target_platform="linux",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    plan = MakeConfig(driver="dummy", lang=lang).build_plan(make_env)
    assert plan.docker_container == container

    process = mock.MagicMock()
    process.__enter__.return_value = process
    process.poll.return_value = None
    run = mock.Mock()
    monkeypatch.setattr(make_config.subprocess, "check_call", mock.Mock())
    monkeypatch.setattr(make_config.subprocess, "Popen", mock.Mock(return_value=process))
    monkeypatch.setattr(make_config.subprocess, "run", run)

    plan._run_docker()

    exec_commands = [
        call.args[0]
        for call in run.call_args_list
        if call.args[0][:2] == ["docker", "exec"]
    ]
    assert len(exec_commands) == len(plan.commands)
    for command, args in zip(exec_commands, plan.commands):
        inner_command = shlex.split(command[-1])
        assert inner_command[0] == "env"
        assert {f"{key}={value}" for key, value in cache_env.items()} <= set(
            inner_command
        )
        assert inner_command[-len(args) :] == args


def shared_library_affix() -> tuple[str, str]:
    return {
        "Darwin": ("lib", ".dylib"),
        "Linux": ("lib", ".so"),
        "Windows": ("lib", ".dll"),
    }[platform.system()]


def test_go_linux_amd64(go_driver_root: tuple[Path, Path]) -> None:
    repo_root, driver_root = go_driver_root
    config = MakeEnv(
        ci=False,
        debug=False,
        host_platform="linux",
        host_architecture="amd64",
        target_platform="linux",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    make_config = MakeConfig.model_validate(
        {
            "driver": "godummy",
            "lang": {
                "lang": "go",
                "go-mod-path": "module",
                "build-tags": ["custom_feature"],
                "release-build-tags": ["release_feature"],
            },
        }
    )

    plan = make_config.build_plan(config)
    assert plan.env_vars == {}
    assert plan.commands == [
        [
            "go",
            "-C",
            "module",
            "build",
            "-buildmode=c-shared",
            "-tags=driverlib,timetzdata,custom_feature,release_feature",
            "-ldflags=-w -X github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion=0.1.0",
            "-o",
            "build/libadbc_driver_godummy.so",
            "./pkg",
        ]
    ]
    assert plan.pre_commands == []
    assert plan.artifact_path == (
        driver_root / "module" / "build" / "libadbc_driver_godummy.so"
    )
    assert plan.target_path == driver_root / "build" / "libadbc_driver_godummy.so"
    assert plan.cleanup_paths == [
        driver_root / "module" / "build" / "libadbc_driver_godummy.h"
    ]
    assert plan.docker_container is None

    config.debug = True
    plan = make_config.build_plan(config)
    assert "-tags=driverlib,timetzdata,assert,custom_feature" in plan.commands[0]


def test_go_linux_amd64_ci(go_driver_root: tuple[Path, Path]) -> None:
    repo_root, driver_root = go_driver_root
    config = MakeEnv(
        ci=True,
        debug=False,
        host_platform="linux",
        host_architecture="amd64",
        target_platform="linux",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    make_config = MakeConfig(
        driver="godummy", lang=LangGo(lang="go", go_mod_path="module")
    )

    plan = make_config.build_plan(config)
    assert plan.env_vars == {"GOWORK": "off"}
    assert plan.pre_commands == [["go", "-C", "module", "mod", "vendor"]]
    assert plan.commands == [
        [
            "go",
            "-C",
            "module",
            "build",
            "-buildmode=c-shared",
            "-tags=driverlib,timetzdata",
            "-ldflags=-w -X github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion=0.1.0 -linkmode external -extldflags=-Wl,--version-script=/only-export-adbc.ld",
            "-o",
            "build/libadbc_driver_godummy.so",
            "./pkg",
        ]
    ]
    assert plan.docker_container == "manylinux"


def test_go_windows_amd64(go_driver_root: tuple[Path, Path]) -> None:
    repo_root, driver_root = go_driver_root
    config = MakeEnv(
        ci=False,
        debug=False,
        host_platform="windows",
        host_architecture="amd64",
        target_platform="windows",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )
    make_config = MakeConfig(
        driver="godummy", lang=LangGo(lang="go", go_mod_path="module")
    )

    plan = make_config.build_plan(config)
    assert "build/libadbc_driver_godummy.dll" in plan.commands[0]
    assert plan.target_path == driver_root / "build" / "libadbc_driver_godummy.dll"
    assert plan.docker_container is None


def test_rust_linux_amd64(rust_driver_root: tuple[Path, Path]) -> None:
    repo_root, driver_root = rust_driver_root
    plat = "linux"
    arch = "amd64"
    config = MakeEnv(
        ci=False,
        debug=False,
        host_platform=plat,
        host_architecture=arch,
        target_platform=plat,
        target_architecture=arch,
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )

    make_config = MakeConfig(driver="foobar", lang=LangRust(lang="rust", features=[]))
    plan = make_config.build_plan(config)
    assert plan.env_vars == {}
    assert plan.commands == [["cargo", "build", "--release"]]
    assert plan.artifact_path == driver_root / "target" / "release" / "libadbc_dummy.so"
    assert plan.docker_container is None

    make_config = MakeConfig(
        driver="foobar", lang=LangRust(lang="rust", features=["foobar"])
    )
    plan = make_config.build_plan(config)
    assert plan.commands == [["cargo", "build", "--release", "--features", "foobar"]]

    config.debug = True
    plan = make_config.build_plan(config)
    assert plan.commands == [["cargo", "build", "--features", "foobar"]]
    assert plan.docker_container is None

    config.debug = False
    config.ci = True
    plan = make_config.build_plan(config)
    assert plan.docker_container == "manylinux-rust"


def test_rust_macos_arm64(rust_driver_root: tuple[Path, Path]) -> None:
    repo_root, driver_root = rust_driver_root
    plat = "macos"
    arch = "arm64"
    config = MakeEnv(
        ci=False,
        debug=False,
        host_platform=plat,
        host_architecture=arch,
        target_platform=plat,
        target_architecture=arch,
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )

    make_config = MakeConfig(driver="foobar", lang=LangRust(lang="rust", features=[]))
    plan = make_config.build_plan(config)
    assert plan.env_vars == {
        "CGO_CFLAGS": "-mmacos-version-min=13.0",
        "CGO_LDFLAGS": "-mmacos-version-min=13.0",
        "MACOSX_DEPLOYMENT_TARGET": "13.0",
    }
    assert plan.commands == [["cargo", "build", "--release"]]
    assert (
        plan.artifact_path == driver_root / "target" / "release" / "libadbc_dummy.dylib"
    )
    assert plan.docker_container is None

    make_config = MakeConfig(
        driver="foobar", lang=LangRust(lang="rust", features=["foobar"])
    )
    plan = make_config.build_plan(config)
    assert plan.commands == [["cargo", "build", "--release", "--features", "foobar"]]

    config.debug = True
    plan = make_config.build_plan(config)
    assert plan.commands == [["cargo", "build", "--features", "foobar"]]
    assert plan.docker_container is None


def test_rust_windows_amd64(rust_driver_root: tuple[Path, Path]) -> None:
    repo_root, driver_root = rust_driver_root
    plat = "windows"
    arch = "amd64"
    config = MakeEnv(
        ci=False,
        debug=False,
        host_platform=plat,
        host_architecture=arch,
        target_platform=plat,
        target_architecture=arch,
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )

    make_config = MakeConfig(driver="foobar", lang=LangRust(lang="rust", features=[]))
    plan = make_config.build_plan(config)
    assert plan.env_vars == {}
    assert plan.commands == [["cargo", "build", "--release"]]
    assert plan.artifact_path == driver_root / "target" / "release" / "adbc_dummy.dll"
    assert plan.target_path == driver_root / "build" / "libadbc_driver_foobar.dll"
    assert plan.docker_container is None

    make_config = MakeConfig(
        driver="foobar", lang=LangRust(lang="rust", features=["foobar"])
    )
    plan = make_config.build_plan(config)
    assert plan.commands == [["cargo", "build", "--release", "--features", "foobar"]]

    config.debug = True
    plan = make_config.build_plan(config)
    assert plan.commands == [["cargo", "build", "--features", "foobar"]]
    assert plan.docker_container is None


def test_rust_cross_compile(rust_driver_root: tuple[Path, Path]) -> None:
    # We only support xcompling from macOS to Linux
    repo_root, driver_root = rust_driver_root
    plat = "macos"
    arch = "arm64"
    config = MakeEnv(
        ci=False,
        debug=False,
        host_platform=plat,
        host_architecture=arch,
        target_platform="linux",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )

    make_config = MakeConfig(driver="foobar", lang=LangRust(lang="rust", features=[]))
    plan = make_config.build_plan(config)
    assert plan.env_vars == {}
    assert plan.commands == [["cargo", "build", "--release"]]
    assert plan.artifact_path == driver_root / "target" / "release" / "libadbc_dummy.so"
    assert plan.docker_container == "manylinux-rust"

    make_config = MakeConfig(
        driver="foobar", lang=LangRust(lang="rust", features=["foobar"])
    )
    plan = make_config.build_plan(config)
    assert plan.commands == [["cargo", "build", "--release", "--features", "foobar"]]

    config.debug = True
    plan = make_config.build_plan(config)
    assert plan.commands == [["cargo", "build", "--features", "foobar"]]
    assert plan.docker_container == "manylinux-rust"


def test_script_windows_amd64(rust_driver_root: tuple[Path, Path]) -> None:
    repo_root, driver_root = rust_driver_root
    config = MakeEnv(
        ci=True,
        debug=False,
        host_platform="windows",
        host_architecture="amd64",
        target_platform="windows",
        target_architecture="amd64",
        repo_root=repo_root,
        driver_root=driver_root,
        version="0.1.0",
    )

    make_config = MakeConfig(
        driver="foobar", lang=LangScript(lang="script", toolchain="cpp")
    )
    plan = make_config.build_plan(config)
    assert plan.env_vars == {}
    assert plan.commands == [
        [
            r"C:\Program Files\Git\bin\bash.EXE",
            "./ci/scripts/build.sh",
            "release",
            "windows",
            "amd64",
        ]
    ]
    assert plan.artifact_path is None
    assert plan.target_path == driver_root / "build" / "libadbc_driver_foobar.dll"
    assert plan.docker_container is None


@debug_subprocess
def test_go_actual_release(go_driver_root: tuple[Path, Path]) -> None:
    _, driver_root = go_driver_root
    prefix, suffix = shared_library_affix()
    env = os.environ.copy()
    env.pop("CI", None)
    result = subprocess.check_output(
        ["adbc-make", "-a"],
        cwd=driver_root,
        text=True,
        stderr=subprocess.STDOUT,
        env=env,
    )
    assert "* go -C module build" in result
    assert "* docker" not in result
    assert (driver_root / "build" / f"{prefix}adbc_driver_godummy{suffix}").is_file()
    assert not (driver_root / "module" / "build" / "libadbc_driver_godummy.h").exists()


@debug_subprocess
def test_go_actual_release_ci(go_driver_root: tuple[Path, Path]) -> None:
    _, driver_root = go_driver_root
    prefix, suffix = shared_library_affix()
    result = subprocess.check_output(
        ["adbc-make", "-a", "check", "CI=true"],
        cwd=driver_root,
        text=True,
        stderr=subprocess.STDOUT,
    )
    if platform.system() == "Linux":
        assert "* go -C module mod vendor" in result
        assert "* docker exec" in result
    else:
        assert "* go -C module mod vendor" not in result
        assert "* docker" not in result
    if platform.system() != "Windows":
        assert "-linkmode external" in result
    assert (driver_root / "build" / f"{prefix}adbc_driver_godummy{suffix}").is_file()
    assert not (driver_root / "module" / "build" / "libadbc_driver_godummy.h").exists()


@debug_subprocess
def test_rust_actual_debug(rust_driver_root: tuple[Path, Path]) -> None:
    prefix, suffix = shared_library_affix()
    result = subprocess.check_output(
        ["adbc-make", "-a", "DEBUG=true"],
        cwd=rust_driver_root[1],
        text=True,
        stderr=subprocess.STDOUT,
    )
    assert "Finished `dev` profile" in result
    assert "* cargo build" in result
    assert "* docker" not in result
    assert (
        rust_driver_root[1] / "build" / f"{prefix}adbc_driver_rustdummy{suffix}"
    ).is_file()


@debug_subprocess
def test_rust_actual_release(rust_driver_root: tuple[Path, Path]) -> None:
    prefix, suffix = shared_library_affix()
    env = os.environ.copy()
    if "CI" in env:
        del env["CI"]
    result = subprocess.check_output(
        ["adbc-make", "-a"],
        cwd=rust_driver_root[1],
        text=True,
        stderr=subprocess.STDOUT,
        env=env,
    )
    assert "Finished `release` profile" in result
    assert "* cargo build" in result
    assert "* docker" not in result
    assert (
        rust_driver_root[1] / "build" / f"{prefix}adbc_driver_rustdummy{suffix}"
    ).is_file()


@debug_subprocess
def test_rust_actual_release_ci(rust_driver_root: tuple[Path, Path]) -> None:
    prefix, suffix = shared_library_affix()
    result = subprocess.check_output(
        ["adbc-make", "-a", "check", "CI=true"],
        cwd=rust_driver_root[1],
        text=True,
        stderr=subprocess.STDOUT,
    )
    print(result)
    # TODO: force-tag and check version too
    assert "Finished `release` profile" in result
    if platform.system() == "Linux":
        assert "* docker exec" in result
        assert "-c env cargo build" in result
    else:
        assert "* docker exec" not in result
        assert "* cargo build" in result
    assert (
        rust_driver_root[1] / "build" / f"{prefix}adbc_driver_rustdummy{suffix}"
    ).is_file()
