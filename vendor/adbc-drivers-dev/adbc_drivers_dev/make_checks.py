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

"""Post-build checks for compiled driver binaries."""

import os
import subprocess
from pathlib import Path

import packaging.version

from .make_config import MakeConfig, MakeEnv

_LINUX_RUNTIME_DEPENDENCIES = {
    "libc.so.6",
    "libdl.so.2",
    "libgcc_s.so.1",
    "libm.so.6",
    "libpthread.so.0",
    "libresolv.so.2",
    "librt.so.1",
    "libstdc++.so.6",
    "linux-vdso.so.1",
}
_LINUX_LOADERS = {
    "amd64": "/lib64/ld-linux-x86-64.so.2",
    "arm64": "/lib/ld-linux-aarch64.so.1",
}
_MACOS_RUNTIME_DEPENDENCIES = {
    "/usr/lib/libSystem.B.dylib",
    "/usr/lib/libc++.1.dylib",
    "/usr/lib/libiconv.2.dylib",
    "/usr/lib/libresolv.9.dylib",
    "/System/Library/Frameworks/CoreFoundation.framework/Versions/A/CoreFoundation",
    "/System/Library/Frameworks/CoreServices.framework/Versions/A/CoreServices",
    "/System/Library/Frameworks/Security.framework/Versions/A/Security",
    "/System/Library/Frameworks/SystemConfiguration.framework/Versions/A/SystemConfiguration",
}
_WINDOWS_RUNTIME_DEPENDENCIES = {
    "ADVAPI32.DLL",
    "API-MS-WIN-CORE-SYNCH-L1-2-0.DLL",
    "API-MS-WIN-CRT-CONVERT-L1-1-0.DLL",
    "API-MS-WIN-CRT-ENVIRONMENT-L1-1-0.DLL",
    "API-MS-WIN-CRT-FILESYSTEM-L1-1-0.DLL",
    "API-MS-WIN-CRT-HEAP-L1-1-0.DLL",
    "API-MS-WIN-CRT-LOCALE-L1-1-0.DLL",
    "API-MS-WIN-CRT-MATH-L1-1-0.DLL",
    "API-MS-WIN-CRT-MULTIBYTE-L1-1-0.DLL",
    "API-MS-WIN-CRT-PRIVATE-L1-1-0.DLL",
    "API-MS-WIN-CRT-RUNTIME-L1-1-0.DLL",
    "API-MS-WIN-CRT-STDIO-L1-1-0.DLL",
    "API-MS-WIN-CRT-STRING-L1-1-0.DLL",
    "API-MS-WIN-CRT-TIME-L1-1-0.DLL",
    "API-MS-WIN-CRT-UTILITY-L1-1-0.DLL",
    "BCRYPT.DLL",
    "BCRYPTPRIMITIVES.DLL",
    "CRYPT32.DLL",
    "IPHLPAPI.DLL",
    "KERNEL32.DLL",
    "KERNELBASE.DLL",
    "MSVCP140.DLL",
    "NTDLL.DLL",
    "RSTRTMGR.DLL",
    "SECUR32.DLL",
    "SHELL32.DLL",
    "UCRTBASE.DLL",
    "USER32.DLL",
    "USERENV.DLL",
    "VCRUNTIME140.DLL",
    "VCRUNTIME140_1.DLL",
    "WS2_32.DLL",
    "WSOCK32.DLL",
}


def _read_linux_symbols(binary: Path) -> list[str]:
    return (
        subprocess.check_output(
            ["nm", "--demangle", "--dynamic", str(binary)], text=True
        )
        .strip()
        .splitlines()
    )


def _read_linux_dependencies(binary: Path) -> list[str]:
    return subprocess.check_output(["ldd", str(binary)], text=True).splitlines()


def _read_macos_symbols(binary: Path) -> list[str]:
    return (
        subprocess.check_output(["nm", "-gU", str(binary)], text=True)
        .strip()
        .splitlines()
    )


def _read_macos_dependencies(binary: Path) -> list[str]:
    return subprocess.check_output(["otool", "-L", str(binary)], text=True).splitlines()


def _read_windows_symbols(binary: Path) -> list[str]:
    return (
        subprocess.check_output(["dumpbin", "/exports", str(binary)], text=True)
        .strip()
        .splitlines()
    )


def _read_windows_dependencies(binary: Path) -> list[str]:
    return (
        subprocess.check_output(["dumpbin", "/dependents", str(binary)], text=True)
        .strip()
        .splitlines()
    )


def _read_linux_symbols_in_docker(
    make_env: MakeEnv, make_config: MakeConfig, binary: Path
) -> list[str]:
    rel_binary = binary.resolve().relative_to(make_env.repo_root.resolve())
    env = {
        **os.environ,
        "SOURCE_ROOT": str(make_env.repo_root),
        "DOCKER_DEFAULT_PLATFORM": (
            f"{make_env.target_platform}/{make_env.target_architecture}"
        ),
        "MANYLINUX": make_config.manylinux,
    }
    return (
        subprocess.check_output(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "manylinux",
                "nm",
                "--demangle",
                "--dynamic",
                f"/source/{rel_binary.as_posix()}",
            ],
            cwd=Path(__file__).parent,
            env=env,
            text=True,
        )
        .strip()
        .splitlines()
    )


def _read_linux_dependencies_in_docker(
    make_env: MakeEnv, make_config: MakeConfig, binary: Path
) -> list[str]:
    rel_binary = binary.resolve().relative_to(make_env.repo_root.resolve())
    env = {
        **os.environ,
        "SOURCE_ROOT": str(make_env.repo_root),
        "DOCKER_DEFAULT_PLATFORM": (
            f"{make_env.target_platform}/{make_env.target_architecture}"
        ),
        "MANYLINUX": make_config.manylinux,
    }
    return subprocess.check_output(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "manylinux",
            "ldd",
            f"/source/{rel_binary.as_posix()}",
        ],
        cwd=Path(__file__).parent,
        env=env,
        text=True,
    ).splitlines()


def _extract_linux_dependencies(output: list[str]) -> set[str]:
    dependencies = set()
    for raw_line in output:
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split()
        dependencies.add(parts[0])
    return dependencies


def check_runtime_dependencies(
    dependencies: set[str], binary: Path, allowed: set[str]
) -> None:
    unexpected = dependencies - allowed
    if unexpected:
        details = ", ".join(sorted(unexpected))
        raise RuntimeError(f"{binary} has unexpected runtime dependencies: {details}")


def _extract_macos_dependencies(output: list[str]) -> set[str]:
    dependencies = set()
    skipped_install_name = False
    for raw_line in output[1:]:
        line = raw_line.strip()
        if not line:
            continue
        if not skipped_install_name:
            skipped_install_name = True
            continue
        dependency, _, _ = line.partition(" (")
        dependencies.add(dependency)
    return dependencies


def _extract_windows_dependencies(output: list[str]) -> set[str]:
    dependencies = set()
    in_dependencies = False
    for raw_line in output:
        line = raw_line.strip()
        if line in {
            "Image has the following dependencies:",
            "Image has the following delay load dependencies:",
        }:
            in_dependencies = True
            continue
        if line == "Summary":
            in_dependencies = False
            continue
        if in_dependencies and line.upper().endswith(".DLL"):
            dependencies.add(line.upper())
    return dependencies


def _extract_exported_linux_symbols(symbols: list[str]) -> list[str]:
    exported_symbols = []
    for symbol in symbols:
        if " T " not in symbol:
            continue
        _, _, name = symbol.partition(" T ")
        exported_symbols.append(name)
    return exported_symbols


def _extract_exported_macos_symbols(symbols: list[str]) -> list[str]:
    exported_symbols = []
    for symbol in symbols:
        if " T " not in symbol:
            continue
        _, _, name = symbol.partition(" T ")
        exported_symbols.append(name.removeprefix("_"))
    return exported_symbols


def _extract_exported_windows_symbols(symbols: list[str]) -> list[str]:
    exported_symbols = []
    name_column = None
    for symbol in symbols:
        if symbol.strip().split() == ["ordinal", "hint", "RVA", "name"]:
            name_column = symbol.index("name")
            continue
        if name_column is None:
            continue
        if symbol.strip() == "Summary":
            break

        name = symbol[name_column:].strip().partition(" ")[0]
        if name:
            exported_symbols.append(name)
    return exported_symbols


def check_required_symbols(
    make_config: MakeConfig, exported_symbols: list[str], binary: Path, driver: str
) -> None:
    """Check that required symbols are present in exported symbols."""
    driver_name = "".join(part.capitalize() for part in driver.lower().split("-"))
    missing_symbols = set()
    required_symbols = ["AdbcDriverInit"]
    if not make_config.checks.disable_driver_entrypoint_check:
        required_symbols.append(f"AdbcDriver{driver_name}Init")

    for required_symbol in required_symbols:
        if required_symbol not in exported_symbols:
            missing_symbols.add(required_symbol)
    if missing_symbols:
        raise RuntimeError(
            f"{', '.join(missing_symbols)} should be exported from {binary}"
        )


def check_disallowed_symbols(
    exported_symbols: list[str], binary: Path, driver: str
) -> None:
    """Check that disallowed symbols are not exported."""
    bad_symbols = [
        symbol for symbol in exported_symbols if not symbol.startswith("Adbc")
    ]
    if bad_symbols:
        raise RuntimeError(
            f"{', '.join(bad_symbols[:3])}... ({len(bad_symbols)} symbols total) should not be exported from {binary}"
        )


def check_linux_libc_requirement(symbols: list[str], manylinux: str) -> None:
    """Check that required libc symbols are compatible with the manylinux policy."""
    limits = {
        "manylinux2014": ("2.17", "3.4.19"),
        "manylinux_2_28": ("2.28", "3.4.32"),
    }
    try:
        glibc_max, glibcxx_max = limits[manylinux.lower()]
    except KeyError as err:
        raise ValueError(f"Unsupported manylinux policy: {manylinux}") from err

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


def _check_linux(make_env: MakeEnv, make_config: MakeConfig, binary: Path) -> None:
    if make_env.host_platform == "linux":
        symbols = _read_linux_symbols(binary)
        dependencies = _read_linux_dependencies(binary)
    elif make_env.use_docker:
        symbols = _read_linux_symbols_in_docker(make_env, make_config, binary)
        dependencies = _read_linux_dependencies_in_docker(make_env, make_config, binary)
    else:
        raise RuntimeError(
            "Cannot run Linux compatibility checks on non-Linux host without Docker"
        )
    exported_symbols = _extract_exported_linux_symbols(symbols)
    check_required_symbols(make_config, exported_symbols, binary, make_config.driver)
    check_disallowed_symbols(exported_symbols, binary, make_config.driver)
    check_linux_libc_requirement(symbols, make_config.manylinux)
    try:
        loader = _LINUX_LOADERS[make_env.target_architecture]
    except KeyError as err:
        raise ValueError(
            f"Unsupported Linux architecture: {make_env.target_architecture}"
        ) from err
    allowed_dependencies = (
        _LINUX_RUNTIME_DEPENDENCIES
        | {loader}
        | set(make_config.additional_runtime_dependencies.get("linux", []))
    )
    check_runtime_dependencies(
        _extract_linux_dependencies(dependencies),
        binary,
        allowed_dependencies,
    )


def _check_macos_deployment_target(binary: Path) -> None:
    output = subprocess.check_output(["otool", "-l", str(binary)], text=True)
    minos = None
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("minos"):
            _, _, minos = line.partition(" ")
            break

    if minos is None:
        raise RuntimeError("Could not determine minimum macOS version")

    version = packaging.version.Version(minos)
    max_version = packaging.version.Version("13.0")
    if version > max_version:
        raise RuntimeError(
            f"{binary} requires macOS {version} but {max_version} was expected at most"
        )


def _check_macos(make_config: MakeConfig, binary: Path) -> None:
    symbols = _read_macos_symbols(binary)
    dependencies = _read_macos_dependencies(binary)
    exported_symbols = _extract_exported_macos_symbols(symbols)
    check_required_symbols(make_config, exported_symbols, binary, make_config.driver)
    check_disallowed_symbols(exported_symbols, binary, make_config.driver)
    _check_macos_deployment_target(binary)
    allowed_dependencies = _MACOS_RUNTIME_DEPENDENCIES | set(
        make_config.additional_runtime_dependencies.get("macos", [])
    )
    check_runtime_dependencies(
        _extract_macos_dependencies(dependencies),
        binary,
        allowed_dependencies,
    )


def _check_windows(make_config: MakeConfig, binary: Path) -> None:
    symbols = _read_windows_symbols(binary)
    dependencies = _read_windows_dependencies(binary)
    exported_symbols = _extract_exported_windows_symbols(symbols)
    check_required_symbols(make_config, exported_symbols, binary, make_config.driver)
    allowed_dependencies = _WINDOWS_RUNTIME_DEPENDENCIES | {
        dependency.upper()
        for dependency in make_config.additional_runtime_dependencies.get("windows", [])
    }
    check_runtime_dependencies(
        _extract_windows_dependencies(dependencies),
        binary,
        allowed_dependencies,
    )
    # Do not check disallowed symbols on Windows - we don't have a good way of
    # hiding symbols as we do on Linux/macOS


def check(make_env: MakeEnv, make_config: MakeConfig, binary: Path) -> None:
    if make_env.target_platform == "linux":
        _check_linux(make_env, make_config, binary)
    elif make_env.target_platform == "macos":
        _check_macos(make_config, binary)
    elif make_env.target_platform == "windows":
        _check_windows(make_config, binary)
    else:
        raise ValueError(f"Unknown target platform: {make_env.target_platform}")
