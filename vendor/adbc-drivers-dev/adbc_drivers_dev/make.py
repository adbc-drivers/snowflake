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

# LOCAL MODIFICATIONS: Snowflake vendors this file with a SemVer-safe git revision
# suffix and writable Go caches for arbitrary-UID manylinux builds.

"""
A build script for ADBC drivers using doit.

See: https://pydoit.org/
"""

import os
import platform
import shlex
import subprocess
import sys
from pathlib import Path

import doit
import packaging.version

match platform.system():
    case "Darwin":
        EXT = "dylib"
        PLATFORM = "macos"
    case "Linux":
        EXT = "so"
        PLATFORM = "linux"
    case "Windows":
        EXT = "dll"
        PLATFORM = "windows"
    case _:
        raise RuntimeError(f"Unsupported platform: {platform.system()}")


DOIT_CONFIG = {
    "default_tasks": ["build"],
}
SMUGGLE_VARS = {"CGO_CFLAGS", "CGO_LDFLAGS", "PROTOC"}


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


def append_flags(env: dict[str, str], var: str, flags: str) -> None:
    if var in env:
        env[var] += " " + flags
    else:
        env[var] = flags


def architecture() -> str:
    match platform.machine():
        case "AMD64":
            return "amd64"
        case "aarch64":
            return "arm64"
        case "arm64":
            return "arm64"
        case "arm64v8":
            return "arm64"
        case "x86_64":
            return "amd64"
        case _:
            raise ValueError(f"{platform.machine()} is not a recognized architecture")


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
    print("!", *args, **kwargs, file=sys.stderr)


def detect_version(
    driver_root: Path,
    *,
    strict: bool = False,
) -> str:
    repo_root = driver_root
    while not (repo_root / ".git").is_dir():
        if repo_root.parent == repo_root:
            raise ValueError(f"{driver_root} is not in a git repository")
        repo_root = repo_root.parent

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


def maybe_build_docker(
    *,
    repo_root: Path,
    driver_root: Path,
    env: dict[str, str],
    args: list[str],
    ci: bool,
    container: str,
) -> None:
    if not ci or platform.system() != "Linux" or to_bool(get_var("DEBUG", "False")):
        check_call(args, cwd=driver_root, env=env)
        return

    env = env.copy()
    env["SOURCE_ROOT"] = str(repo_root)
    env["ARCH"] = architecture()

    volumes = get_var("ADDITIONAL_VOLUMES", "")
    if volumes:
        volumes = volumes.split(",")

    # Some env vars need to be explicitly propagated into Docker
    smuggle_env = ""
    for var in SMUGGLE_VARS:
        if var in env:
            smuggle_env += f'{var}="{shlex.quote(env[var])}" '
        elif var in os.environ:
            smuggle_env += f'{var}="{shlex.quote(os.environ[var])}" '

    command = [
        "docker",
        "compose",
        "run",
        "--rm",
        "--user",
        str(os.getuid()),
    ]

    for volume in volumes:
        command.extend(["-v", volume])

    # The manylinux image runs the host UID, which may not exist in /etc/passwd.
    # Go then falls back to the root-owned /go module cache. Keep both caches in
    # /tmp, which is writable for arbitrary UIDs on GitHub-hosted runners.
    if container == "manylinux":
        command.extend(
            [
                "--env",
                "GOCACHE=/tmp/go-build",
                "--env",
                "GOMODCACHE=/tmp/go-mod",
                "--env",
                "GOPATH=/tmp/go",
            ]
        )

    command.extend(
        [
            container,
            "--",
            "bash",
            "-c",
            f"cd /source/{driver_root.relative_to(repo_root)} && env {smuggle_env} {' '.join(shlex.quote(arg) for arg in args)}",
        ]
    )
    check_call(command, cwd=Path(__file__).parent, env=env)


def build_go(
    repo_root: Path,
    driver_root: Path,
    driver: str,
    target: str,
    *,
    ci: bool = False,
) -> None:
    strict = to_bool(get_var("RELEASE", "false"))
    version = detect_version(driver_root, strict=strict)
    (repo_root / "build").mkdir(exist_ok=True)

    # Embed the version in the library
    prop = "github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion"
    ldflags = " ".join(
        [
            # Don't exclude symbols (-s) so panics will have symbol information
            # This will exclude DWARF debug tables (-w).
            "-w",
            f"-X {prop}={version}",
        ]
    )

    tags = ["driverlib"]
    if to_bool(get_var("DEBUG", "False")):
        tags.append("assert")

    extra_tags = get_var("BUILD_TAGS", "")
    if extra_tags:
        extra_tags = extra_tags.split(",")
        extra_tags = [tag.strip() for tag in extra_tags]
        extra_tags = [tag for tag in extra_tags if tag]
        tags.extend(extra_tags)

    tags = ",".join(tags)
    tags = "-tags=" + tags

    info("Building", target, "version", version)

    env = {}
    for var in SMUGGLE_VARS:
        if var in os.environ:
            env[var] = os.environ[var]

    if platform.system() == "Darwin":
        append_flags(env, "CGO_CFLAGS", "-mmacosx-version-min=11.0")
        append_flags(env, "CGO_LDFLAGS", "-mmacosx-version-min=11.0")

    if ci and platform.system() == "Linux" and not to_bool(get_var("DEBUG", "False")):
        check_call(["go", "mod", "vendor"], cwd=driver_root)
        ldflags += (
            " -linkmode external -extldflags=-Wl,--version-script=/only-export-adbc.ld"
        )

        # Command differs under Docker so don't invoke this otherwise
        maybe_build_docker(
            repo_root=repo_root,
            driver_root=driver_root,
            env=env,
            args=[
                "go",
                "build",
                "-buildmode=c-shared",
                tags,
                "-o",
                f"/source/build/{target}",
                "-ldflags",
                ldflags,
                "./pkg",
            ],
            ci=ci,
            container="manylinux",
        )
    else:
        check_call(
            [
                "go",
                "build",
                "-buildmode=c-shared",
                tags,
                "-o",
                f"{repo_root / 'build' / target}",
                "-ldflags",
                ldflags,
                "./pkg",
            ],
            cwd=driver_root,
            env=env,
        )

    output = (repo_root / "build" / target).resolve()
    output.chmod(0o755)
    header = output.with_suffix(".h")
    header.unlink(missing_ok=True)


def build_rust(
    repo_root: Path,
    driver_root: Path,
    driver: str,
    target: str,
    *,
    ci: bool = False,
) -> None:
    strict = to_bool(get_var("RELEASE", "false"))
    version = detect_version(driver_root, strict=strict)
    (repo_root / "build").mkdir(exist_ok=True)

    debug = to_bool(get_var("DEBUG", "False"))

    # Note: version embedded in library is determined by Cargo.toml
    # TODO: check that it matches git tag?
    args = []
    if not debug:
        args.append("--release")

    features = []
    extra_features = get_var("FEATURES", "")
    if extra_features:
        extra_features = extra_features.split(",")
        extra_features = [tag.strip() for tag in extra_features]
        extra_features = [tag for tag in extra_features if tag]
        features.extend(extra_features)

    if features:
        args.append("--features")
        args.append(",".join(features))

    info("Building", target, "version", version, "features", features)

    env = {}
    if platform.system() == "Darwin":
        # https://doc.rust-lang.org/nightly/rustc/platform-support/apple-darwin.html#os-version
        env["MACOSX_DEPLOYMENT_TARGET"] = "11.0"

    maybe_build_docker(
        repo_root=repo_root,
        driver_root=driver_root,
        env=env,
        args=["cargo", "build", *args],
        ci=ci,
        container="manylinux-rust",
    )

    lib = driver_root / "target"
    if debug:
        lib = lib / "debug"
    else:
        lib = lib / "release"

    source_target = target
    # Exclusion basically just for Databricks - their crate name is not
    # "adbc_driver_databricks" but rather "databricks_adbc"
    if target_name := get_var("TARGET_NAME", ""):
        source_target = f"lib{target_name}.{EXT}"
    if platform.system() == "Windows":
        source_target = source_target.removeprefix("lib")
    lib = lib / source_target
    info("Copying", lib, "to", repo_root / "build" / target)

    lib.rename(repo_root / "build" / target)
    output = (repo_root / "build" / target).resolve()
    output.chmod(0o755)


def build_script(
    repo_root: Path,
    driver_root: Path,
    driver: str,
    target: str,
    *,
    ci: bool = False,
) -> None:
    strict = to_bool(get_var("RELEASE", "false"))
    version = detect_version(driver_root, strict=strict)
    (repo_root / "build").mkdir(exist_ok=True)

    debug = to_bool(get_var("DEBUG", "False"))

    args = []
    if debug:
        args.append("test")
    else:
        args.append("release")
    args.append(PLATFORM)
    args.append(architecture())

    info("Building", target, "version", version)

    env = {}
    if platform.system() == "Darwin":
        env["MACOSX_DEPLOYMENT_TARGET"] = "11.0"

    args = ["./ci/scripts/build.sh", *args]
    if ci and PLATFORM == "windows":
        # Force use of Git Bash on GitHub Actions
        args = [r"C:\Program Files\Git\bin\bash.EXE", *args]

    toolchain = get_var("TOOLCHAIN", "")
    if not toolchain:
        raise ValueError("Must specify TOOLCHAIN=toolchain for script-based build")

    container = {
        "cpp": "manylinux-cpp",
        "go": "manylinux",
        "rust": "manylinux-rust",
    }.get(toolchain)
    if container is None:
        raise ValueError(f"Unsupported TOOLCHAIN={toolchain} for script-based build")

    # if we're using a script, don't invoke docker for Go; the script itself
    # will invoke docker

    maybe_build_docker(
        repo_root=repo_root,
        driver_root=driver_root,
        env=env,
        args=args,
        ci=ci and toolchain != "go",
        container=container,
    )

    output = (repo_root / "build" / target).resolve()
    output.chmod(0o755)


def check_linux(binary: Path) -> None:
    symbols = check_output(
        [
            "nm",
            "--demangle",
            "--dynamic",
            str(binary),
        ]
    ).splitlines()

    # Make sure only 'Adbc*' symbols are exported
    bad_symbols = []
    for symbol in symbols:
        if " T " not in symbol:
            continue
        _, _, name = symbol.partition(" T ")
        if not name.startswith("Adbc"):
            bad_symbols.append(name)
    if bad_symbols:
        raise RuntimeError(
            f"{', '.join(bad_symbols[:3])}... ({len(bad_symbols)} symbols total) should not be exported from {binary}"
        )

    # Like upstream.  Match manylinux2014's versions.
    # https://peps.python.org/pep-0599/#the-manylinux2014-policy
    manylinux = get_var("MANYLINUX", "manylinux2014").lower()
    if manylinux == "manylinux2014":
        glibc_max = "2.17"
        glibcxx_max = "3.4.19"
    elif manylinux == "manylinux_2_28":
        glibc_max = "2.28"
        glibcxx_max = "3.4.32"

    for symbol in symbols:
        if "@GLIBC_" in symbol:
            version = packaging.version.Version(symbol.partition("@")[2][6:])
            if version > packaging.version.Version(glibc_max):
                raise RuntimeError(
                    f"{symbol} requires too new a glibc (max {glibc_max})"
                )
        elif "@GLIBCXX_" in symbol:
            version = packaging.version.Version(symbol.partition("@")[2][8:])
            if version > packaging.version.Version(glibcxx_max):
                raise RuntimeError(
                    f"{symbol} requires too new a glibcxx (max {glibcxx_max})"
                )


def check_macos(binary: Path) -> None:
    output = check_output(["otool", "-l", str(binary)]).splitlines()
    minos = None
    for line in output:
        line = line.strip()
        if not line.startswith("minos"):
            continue
        _, _, minos = line.partition(" ")
        break

    if minos is None:
        raise RuntimeError("Could not determine minimum macOS version")

    minos = packaging.version.Version(minos)
    maxos = packaging.version.Version("11.0")

    if minos > maxos:
        raise RuntimeError(
            f"{binary} requires macOS {minos} but {maxos} was expected at most"
        )


def check(binary: Path) -> None:
    if platform.system() == "Linux":
        check_linux(binary)
    elif platform.system() == "Darwin":
        check_macos(binary)


def task_build():
    driver = get_var("DRIVER", "")
    if not driver:
        raise ValueError("Must specify DRIVER=driver")

    ci = to_bool(get_var("CI", False))
    lang = get_var("IMPL_LANG", "go").strip().lower()

    repo_root = Path(".").resolve().absolute()
    driver_root = Path(driver)
    if driver_root.is_dir():
        driver_root = driver_root.resolve()
    elif (
        Path("./go.mod").is_file() or Path("./Cargo.toml").is_file() or lang == "script"
    ):
        driver_root = Path(".").resolve()

    # Compute dependencies
    file_deps = []
    extensions = [".go", ".c", ".cc", ".cpp", ".h", ".rs"]
    for dirname, _, filenames in driver_root.walk():
        for filename in filenames:
            if filename in {"go.mod", "go.sum", "Cargo.toml", "Cargo.lock"}:
                file_deps.append(Path(dirname) / filename)
            elif any(filename.endswith(ext) for ext in extensions):
                file_deps.append(Path(dirname) / filename)

    target = f"libadbc_driver_{driver}.{EXT}"

    if lang == "go":
        actions = [
            lambda: build_go(repo_root, driver_root, driver, target, ci=ci),
        ]
    elif lang == "rust":
        actions = [
            lambda: build_rust(repo_root, driver_root, driver, target, ci=ci),
        ]
    elif lang == "script":
        actions = [
            lambda: build_script(repo_root, driver_root, driver, target, ci=ci),
        ]
    else:
        raise ValueError(f"Unsupported LANG={lang}")

    return {
        "actions": actions,
        "file_dep": [str(p) for p in file_deps],
        "targets": [repo_root / "build" / target],
    }


def task_check():
    driver = get_var("DRIVER", "")
    if not driver:
        raise ValueError("Must specify DRIVER=driver")

    repo_root = Path(".").resolve()
    target = repo_root / "build" / f"libadbc_driver_{driver}.{EXT}"

    return {
        "actions": [
            lambda: check(target),
        ],
        "file_dep": [target],
        "targets": [],
    }


def main():
    doit.run(globals())


if __name__ == "__main__":
    main()
