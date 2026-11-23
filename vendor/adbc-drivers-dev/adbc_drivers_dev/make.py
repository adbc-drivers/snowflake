#!/usr/bin/env python3
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

"""
A build script for ADBC drivers using doit.

See: https://pydoit.org/
"""

import functools
import os
import platform
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path

import doit
import packaging.version

from . import make_checks, make_config

HOST_PLATFORM_NAMES = {
    "Darwin": "macos",
    "Linux": "linux",
    "Windows": "windows",
}

PLATFORM_EXTENSIONS = {
    "macos": "dylib",
    "linux": "so",
    "windows": "dll",
}

ARCH_ALIASES = {
    "amd64": "amd64",
    "x86_64": "amd64",
    "x64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
    "arm64v8": "arm64",
}

HOST_SYSTEM = platform.system()
try:
    PLATFORM = HOST_PLATFORM_NAMES[HOST_SYSTEM]
except KeyError as err:
    raise RuntimeError(f"Unsupported platform: {HOST_SYSTEM}") from err


DOIT_CONFIG = {
    "default_tasks": ["build"],
}


def to_bool(value: str | bool) -> bool:
    if value is None:
        return False
    elif isinstance(value, bool):
        return value
    value = value.lower()
    if value in {"1", "true", "yes"}:
        return True
    elif value in {"0", "false", "no"}:
        return False
    raise ValueError(f"Cannot convert {value!r} to bool")


def is_verbose() -> bool:
    return to_bool(get_var("VERBOSE", "False"))


def normalize_arch(value: str) -> str:
    try:
        return ARCH_ALIASES[value.lower()]
    except KeyError as err:
        raise ValueError(f"{value} is not a recognized architecture") from err


def _check_call(f, *args, **kwargs) -> str:
    extra_env = kwargs.pop("env", {})
    if extra_env:
        env = os.environ.copy()
        for k, v in extra_env.items():
            if k in {"CGO_CFLAGS", "CGO_LDFLAGS"}:
                if k in env:
                    env[k] += " " + v
                else:
                    env[k] = v
            elif k in {
                "ADBC_DRIVER_BUILD_VERSION",
                "ARCH",
                "DOCKER_DEFAULT_PLATFORM",
                "GOWORK",
                "MACOSX_DEPLOYMENT_TARGET",
                "SOURCE_ROOT",
            }:
                env[k] = v
            else:
                raise TypeError(f"Unsupported env var override {k}")
        env.update(extra_env)
        kwargs["env"] = env

    if is_verbose():
        # TODO: use log, color
        if kwargs.get("cwd") is not None:
            cwd = kwargs["cwd"]
        else:
            cwd = "."
        print(
            "*",
            f"[{cwd}]",
            " ".join(shlex.quote(arg) for arg in args[0]),
            file=sys.stderr,
        )
        if extra_env:
            for k, v in extra_env.items():
                print("*", "[env]", f"{k}={v}", file=sys.stderr)
    return f(*args, **kwargs, text=True)


def check_call(*args, **kwargs) -> str:
    return _check_call(subprocess.check_call, *args, **kwargs)


def check_output(*args, **kwargs) -> str:
    return _check_call(subprocess.check_output, *args, **kwargs).strip()


def info(*args, **kwargs):
    print("?", *args, **kwargs, file=sys.stderr)


def _find_repo_root(driver_root: Path) -> Path:
    repo_root = driver_root
    git_marker = repo_root / ".git"
    while not (git_marker.is_dir() or git_marker.is_file()):
        if repo_root.parent == repo_root:
            raise ValueError(f"{driver_root} is not in a git repository")
        repo_root = repo_root.parent
        git_marker = repo_root / ".git"
    return repo_root


def detect_version(
    driver_root: Path,
    *,
    strict: bool = False,
) -> str:
    repo_root = _find_repo_root(driver_root)

    prefix = str(driver_root.relative_to(repo_root))
    if prefix == ".":
        prefix = "v"
    else:
        prefix = f"{prefix}/v"

    tags = check_output(
        [
            "git",
            "tag",
            "-l",
            "--no-column",
            "--no-format",
            "--no-color",
            "--sort",
            "-v:refname",
            f"{prefix}*",
        ],
        cwd=repo_root,
    ).splitlines()

    if not tags:
        if strict:
            raise ValueError(f"No tags found for driver {driver_root}")
        # use a version that dbc will still accept, not "unknown" like we used to
        version = "v0.0.1-dev"
    else:
        # sort tags, then find distance from all tags to HEAD
        # the assumption is that this is monotonically increasing, else we have a problem
        versions = []
        for tag in tags:
            version_str = tag[len(prefix) - 1 :]
            version = packaging.version.parse(version_str)
            distance = int(
                check_output(
                    ["git", "rev-list", f"{tag}..HEAD", "--count"], cwd=repo_root
                )
            )
            versions.append((version_str, version, distance, tag))

        versions.sort(key=lambda v: v[1], reverse=True)
        for v, prev in zip(versions, versions[1:]):
            if v[2] > prev[2]:
                raise ValueError(
                    f"Tag {v[0]} is further from HEAD than {prev[0]}, but has a newer version"
                )

        version, parsed_version, count, tag = versions[0]
        if count > 0:
            if strict:
                raise ValueError(
                    f"Driver {driver_root} is not on tag {tag}, but has {count} commits since"
                )
            if parsed_version.is_prerelease or parsed_version.is_devrelease:
                # This is a weird edge case, but just use the previous version (or dev version)
                for v in versions:
                    if not (v[1].is_prerelease or v[1].is_devrelease):
                        version, parsed_version, count, tag = v
                        break
                else:
                    version = "v0.0.1"
                    count = int(
                        check_output(
                            ["git", "rev-list", "HEAD", "--count"], cwd=repo_root
                        )
                    )
            rev = check_output(["git", "rev-parse", "--short", "HEAD"], cwd=repo_root)
            # Prefix the revision so an all-numeric hash with a leading zero is
            # not interpreted as an invalid numeric SemVer identifier.
            version += f"-dev.{count}.g{rev}"

    # Append -dirty if there are uncommitted changes
    dirty = check_output(["git", "status", "--porcelain"], cwd=repo_root).splitlines()
    # Ignore untracked files
    if any(not line.startswith("?? ") for line in dirty):
        if strict:
            info(repo_root, "has uncommitted changes. `git status --porcelain`:")
            for line in dirty:
                info("> ", line)
            raise ValueError(f"{repo_root} has uncommitted changes")
        version += "-dirty"

    return version


def get_var(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is not None:
        return value
    value = doit.get_var(name, default)
    return value


def target_platform() -> str:
    target = get_var("TARGET", "").strip().lower()
    if not target:
        return PLATFORM
    target = target.replace("/", "-")
    platform_name, _, _ = target.partition("-")
    if platform_name != "linux":
        raise ValueError(
            "Cross-compilation only supports Linux targets. "
            f"Got: {platform_name!r}. Use TARGET=linux, TARGET=linux-amd64, or TARGET=linux-arm64"
        )
    return platform_name


def target_architecture() -> str:
    target = get_var("TARGET", "").strip().lower()
    if not target:
        return normalize_arch(platform.machine())
    target = target.replace("/", "-")
    _, sep, arch = target.partition("-")
    if not sep:
        return "amd64"
    return normalize_arch(arch)


def target_extension() -> str:
    try:
        return PLATFORM_EXTENSIONS[target_platform()]
    except KeyError as err:
        raise ValueError(f"Unsupported target platform: {target_platform()}") from err


def should_use_docker() -> bool:
    target = get_var("TARGET", "").strip()
    explicit = get_var("USE_DOCKER", "").strip()

    if target:
        if explicit and not to_bool(explicit):
            raise ValueError(
                "Linux cross-compilation requires Docker; USE_DOCKER=false is not supported with TARGET=linux*"
            )
        return target_platform() == "linux"

    if explicit:
        if to_bool(explicit):
            if platform.system() != "Linux":
                raise ValueError(
                    "USE_DOCKER=true without TARGET is only supported on Linux hosts"
                )
            return True
        return False

    if to_bool(get_var("DEBUG", "False")):
        return False

    # CI on Linux: use Docker (original behavior)
    return to_bool(get_var("CI", False)) and platform.system() == "Linux"


@functools.cache
def _load_build_context() -> tuple[
    make_config.MakeEnv, make_config.MakeConfig, make_config.MakePlan
]:
    strict = to_bool(get_var("RELEASE", "false"))
    driver_root = Path(".").resolve().absolute()
    repo_root = _find_repo_root(driver_root)
    version = detect_version(driver_root, strict=strict)

    make_env = make_config.MakeEnv(
        ci=to_bool(get_var("CI", "false")),
        debug=to_bool(get_var("DEBUG", "False")),
        host_platform=PLATFORM,
        host_architecture=normalize_arch(platform.machine()),
        target_platform=target_platform(),
        target_architecture=target_architecture(),
        repo_root=repo_root,
        driver_root=driver_root,
        version=version,
    )
    with (driver_root / "adbc-make.toml").open("rb") as f:
        raw_make = tomllib.load(f)
    make = make_config.MakeConfig.model_validate(raw_make)
    return make_env, make, make.build_plan(make_env)


def task_build():
    make_env, make, make_plan = _load_build_context()
    driver_root = make_env.driver_root

    # Compute dependencies
    file_deps = []
    file_deps.append(driver_root / "adbc-make.toml")
    extensions = [".go", ".c", ".cc", ".cpp", ".h", ".rs"]
    for dirname, _, filenames in driver_root.walk():
        for filename in filenames:
            if filename in {"go.mod", "go.sum", "Cargo.toml", "Cargo.lock"}:
                file_deps.append(Path(dirname) / filename)
            elif any(filename.endswith(ext) for ext in extensions):
                file_deps.append(Path(dirname) / filename)

    result = {
        "actions": [make_plan.run],
        "file_dep": [str(p) for p in file_deps],
        "targets": [str(make_plan.target_path)],
    }

    # Force rebuild when cross-compiling (don't use doit cache)
    if make_env.is_cross_compile:
        result["uptodate"] = [False]  # codespell:ignore uptodate

    info("Build env:", make_env.model_dump_json())
    info("Build config:", make.model_dump_json())
    info("Build plan:", make_plan.model_dump_json())

    return result


def task_check():
    make_env, make, make_plan = _load_build_context()
    target = make_plan.target_path

    return {
        "actions": [lambda: make_checks.check(make_env, make, target)],
        "task_dep": ["build"],
        "file_dep": [str(target)],
        "targets": [],
    }


def main():
    doit.run(globals())


if __name__ == "__main__":
    main()
