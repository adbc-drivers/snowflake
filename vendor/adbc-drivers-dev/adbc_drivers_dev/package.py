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
Generate a driver package from a manifest and shared library(s).
"""

import argparse
import collections
import dataclasses
import enum
import io
import itertools
import os
import subprocess
import sys
import tarfile
import typing
from pathlib import Path

import tomlkit
from ruamel.yaml import YAML

from .make import detect_version


class Architecture(enum.StrEnum):
    AMD64 = "amd64"  # Also known as x86_64, x64
    ARM64 = "arm64"  # Also known as aarch64, arm64v8


class Platform(enum.StrEnum):
    LINUX = "linux"
    MACOS = "macos"
    WINDOWS = "windows"


@dataclasses.dataclass
class Package:
    name: str
    platform: Platform
    architecture: Architecture
    version: str
    files: dict[str, bytes]


def normalize_driver_name(filename: str) -> str:
    """Convert 'libadbc_driver_redshift.so' to 'redshift'."""
    name, _, _ = filename.partition(".")
    name = name.removeprefix("lib")
    parts = name.split("_")
    if len(parts) != 3:
        raise ValueError(f"Invalid driver name: {name}")
    elif parts[0] != "adbc":
        raise ValueError(f"Invalid driver name: {name}")
    elif parts[1] != "driver":
        raise ValueError(f"Invalid driver name: {name}")
    return parts[2]


def validate_manifest(manifest: dict[str, typing.Any]) -> None:
    for field in ("name", "description", "publisher", "license", "version"):
        if field not in manifest:
            raise ValueError(f"Manifest missing required `{field}`")
        elif not isinstance(manifest[field], tomlkit.items.String):
            raise ValueError(f"Manifest `{field}` must be a string")
        elif manifest[field].unwrap() == "":
            raise ValueError(f"Manifest `{field}` must not be empty")

    if "Files" not in manifest:
        raise ValueError("Manifest missing required `Files` section")

    files = manifest["Files"]
    if "driver" not in files:
        raise ValueError("Manifest missing required `Files.driver`")
    elif not isinstance(files["driver"], tomlkit.items.String):
        raise ValueError("Manifest `Files.driver` must be a string")
    elif files["driver"].unwrap() == "":
        raise ValueError("Manifest `Files.driver` must not be empty")


def generate_go_license(license_template: Path, gomod: Path) -> str:
    # Ignore the current package's license (as it's already included)
    this_package = None
    with gomod.open("r") as source:
        for line in source:
            if line.startswith("module "):
                _, _, this_package = line.strip().partition(" ")
    if not this_package:
        raise RuntimeError("Could not determine module name to ignore")

    license_proc = subprocess.run(
        [
            "go-licenses",
            "report",
            "./...",
            "--ignore",
            this_package,
            "--template",
            str(license_template.absolute()),
        ],
        cwd=license_template.parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if license_proc.returncode != 0:
        print("Failed to generate license", file=sys.stderr)
        print("Stdout:", file=sys.stderr)
        print(license_proc.stdout, file=sys.stderr)
        print("Stderr:", file=sys.stderr)
        print(license_proc.stderr, file=sys.stderr)
        license_proc.check_returncode()
    license_data = license_proc.stdout
    return license_data


def generate_rust_license(license_template: Path) -> str:
    license_proc = subprocess.run(
        [
            "cargo-about",
            "generate",
            "--all-features",
            "--fail",
            "--locked",
            "--target",
            "aarch64-apple-darwin,aarch64-unknown-linux-gnu,x86_64-pc-windows-msvc,x86_64-unknown-linux-gnu",
            str(license_template.absolute()),
        ],
        cwd=license_template.parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if license_proc.returncode != 0:
        print("Failed to generate license", file=sys.stderr)
        print("Stdout:", file=sys.stderr)
        print(license_proc.stdout, file=sys.stderr)
        print("Stderr:", file=sys.stderr)
        print(license_proc.stderr, file=sys.stderr)
        license_proc.check_returncode()
    license_data = license_proc.stdout
    return license_data


def generate_custom_license(license_script: Path) -> str:
    import platform

    args = [str(license_script.absolute())]
    if (
        platform.system() == "Windows"
        and os.environ.get("CI", "").lower().strip() == "true"
    ):
        # Force use of Git Bash on GitHub Actions
        args = [r"C:\Program Files\Git\bin\bash.EXE", *args]
    license_proc = subprocess.run(
        args,
        cwd=license_script.parent.parent.parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if license_proc.returncode != 0:
        print("Failed to generate license", file=sys.stderr)
        print("Stdout:", file=sys.stderr)
        print(license_proc.stdout, file=sys.stderr)
        print("Stderr:", file=sys.stderr)
        print(license_proc.stderr, file=sys.stderr)
        license_proc.check_returncode()
    license_data = license_proc.stdout
    return license_data


def generate_packages(
    manifest: dict[str, typing.Any],
    driver_name: str,
    driver_root: Path,
    drivers: list[tuple[Platform, Architecture, Path]],
    strict: bool = False,
) -> list[Package]:
    version = detect_version(driver_root, strict=strict)
    manifest = manifest.copy()
    manifest["version"] = version
    packages: list[Package] = []

    for platform, architecture, driver_path in drivers:
        files: dict[str, bytes] = {}
        with driver_path.open("rb") as source:
            files[driver_path.name] = source.read()

        manifest["Files"] = {
            "driver": driver_path.name,
        }
        validate_manifest(manifest)
        files["MANIFEST"] = tomlkit.dumps(manifest).encode("utf-8")

        packages.append(
            Package(
                driver_name,
                platform,
                architecture,
                version,
                files,
            )
        )
    return packages


def find_drivers(
    driver_name: str, input_dirs: list[Path]
) -> list[tuple[Platform, Architecture, Path]]:
    drivers: list[tuple[Platform, Architecture, Path]] = []
    for input_dir in input_dirs:
        # Get the architecture and platform from the directory name
        parts = input_dir.name.split("-")
        if len(parts) != 3 or (parts and parts[0] != "drivers"):
            print("Invalid input directory name:", input_dir.name, file=sys.stderr)
            print("Expected format: drivers-<platform>-<architecture>", file=sys.stderr)
            return 1

        platform = Platform(parts[1].lower())
        architecture = Architecture(parts[2].lower())
        for driver_path in itertools.chain(
            *(input_dir.rglob(f"*.{ext}") for ext in ("dll", "dylib", "so"))
        ):
            # Ignore drivers that don't match the given manifest
            if normalize_driver_name(driver_path.name) != driver_name:
                continue
            drivers.append((platform, architecture, driver_path))
            print(f"Found {platform}_{architecture} driver:", driver_path)
    return drivers


def main():
    # Assume we're being run from a GitHub action where the directory
    # structure is as follows
    # drivers-linux-amd64/libadbc_driver_mssql.so
    # drivers-macos_11_0-arm64/libadbc_driver_mssql.so
    # Use the directory structure to infer the architecture and platform

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        type=Path,
        help="Directory to write generated packages to",
    )
    parser.add_argument(
        "--name",
        required=True,
        help="The driver name",
    )
    parser.add_argument(
        "--root",
        required=True,
        type=Path,
        help="Path to the driver in version control (to infer version)",
    )
    parser.add_argument(
        "--release",
        action="store_true",
        help="This is a release (be more strict)",
    )
    parser.add_argument(
        "--manifest-template",
        required=True,
        type=Path,
        help="The template manifest",
    )
    parser.add_argument(
        "input",
        nargs="+",
        type=Path,
        help="Directories containing input drivers (must follow naming convention of CI-generated artifacts)",
    )

    args = parser.parse_args()

    # Discover drivers in the input directories
    drivers = find_drivers(args.name, args.input)

    args.output.mkdir(exist_ok=True, parents=True)

    # ------------------------------------------------------------
    # Generate license
    # ------------------------------------------------------------
    license_data = None
    notice_file = args.manifest_template.with_name("NOTICE.txt")
    if not notice_file.is_file():
        notice_file = args.manifest_template.parent.parent / "NOTICE.txt"
    if not notice_file.is_file():
        raise RuntimeError(f"NOTICE.txt ({notice_file}) is missing")

    license_script = args.manifest_template.parent / "ci/scripts/generate_license.sh"
    license_template = args.manifest_template.with_name("license.tpl")
    if license_script.is_file():
        license_data = generate_custom_license(license_script)
    elif license_template.is_file():
        if (gomod := args.manifest_template.with_name("go.mod")).is_file():
            license_data = generate_go_license(license_template, gomod)
        elif args.manifest_template.with_name("Cargo.toml").is_file():
            license_data = generate_rust_license(license_template)
        else:
            raise RuntimeError("Could not determine how to generate license")
    else:
        raise RuntimeError(f"License template ({license_template}) is missing")

    # ------------------------------------------------------------
    # Generate packages
    # ------------------------------------------------------------
    with args.manifest_template.open("rb") as source:
        driver_manifest = tomlkit.load(source)

    # Track some data for the bucket manifest
    pkginfo_version = collections.defaultdict(lambda: {"packages": []})
    bucket_manifest_driver_entry = {
        "name": driver_manifest["name"].unwrap(),
        # "description" is non-standard so we're using it just to populate a
        # description for the driver index. dbc ignores this field so it won't be
        # present when the driver is installed. We fall back to name if not found.
        "description": (
            driver_manifest["description"].unwrap()
            if "description" in driver_manifest
            else driver_manifest["name"].unwrap()
        ),
        "license": driver_manifest["license"].unwrap(),
        "path": args.name,
        "urls": ["https://adbc-drivers.org"],
    }

    for package in generate_packages(
        driver_manifest,
        args.name,
        args.root,
        drivers,
        strict=args.release,
    ):
        print(
            "Generating",
            package.name,
            package.platform,
            package.architecture,
            package.version,
        )

        if license_data:
            package.files["LICENSE"] = license_data
        else:
            raise RuntimeError("LICENSE is missing")

        if notice_file:
            with notice_file.open("rb") as source:
                package.files["NOTICE"] = source.read()

        filename = f"{package.name}_{package.platform}_{package.architecture}_{package.version}.tar.gz"
        subdir = args.output / args.name / package.version
        subdir.mkdir(exist_ok=True, parents=True)
        output = subdir / filename
        print("Output:", output)
        # TODO: use zstd
        with tarfile.open(output, "w:gz") as tar:
            for name, data in package.files.items():
                tarinfo = tarfile.TarInfo(name)
                tarinfo.size = len(data)
                tarinfo.mtime = 0
                tarinfo.mode = 0o644
                tar.addfile(tarinfo, io.BytesIO(data))

        pkginfo_version[package.version]["version"] = package.version
        pkginfo_version[package.version]["packages"].append(
            {
                "platform": f"{package.platform}_{package.architecture}",
                "url": f"{args.name}/{package.version}/{filename}",
            }
        )
    bucket_manifest_driver_entry["pkginfo"] = [v for v in pkginfo_version.values()]
    bucket_manifest = {
        "drivers": [bucket_manifest_driver_entry],
    }

    yaml = YAML()
    with (args.output / "manifest.yaml").open("w") as sink:
        yaml.dump(bucket_manifest, sink)
    print("Generated manifest.yaml")
    yaml.dump(bucket_manifest, sys.stdout)

    return 0


if __name__ == "__main__":
    sys.exit(main())
