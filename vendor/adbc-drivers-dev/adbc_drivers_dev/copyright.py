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

import argparse
import fnmatch
import re
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path, help="Root directory of repository")

    args = parser.parse_args()
    root = args.root.resolve()

    pattern = r"Copyright \(c\) ([0-9]{4}-)?[0-9]{4} Columnar Technologies Inc\. +All rights reserved\."
    header = re.compile(pattern.encode())

    # ------------------------------------------------------------
    # Load the exclusion file if present
    # ------------------------------------------------------------
    exclusion_file = root / ".rat-excludes"
    exclusions = []
    if exclusion_file.is_file():
        with exclusion_file.open("r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    exclusions.append(line)

    # ------------------------------------------------------------
    # Generate a tarball so we can respect .gitignore
    # ------------------------------------------------------------
    # Determine what commit to use; if unstaged changes are present, use
    # git stash to create a temporary commit.
    commit = subprocess.check_output(
        ["git", "stash", "create"], cwd=root, text=True
    ).strip()
    if not commit:
        # No unstaged changes.
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip()

    # ------------------------------------------------------------
    # Check the archive
    # ------------------------------------------------------------
    exitcode = 0
    with tempfile.TemporaryDirectory() as scratch:
        scratch = Path(scratch).resolve()
        archive = scratch / "rat.tar"
        subprocess.check_call(
            ["git", "archive", "--format=tgz", f"--output={archive}", commit], cwd=root
        )

        with tarfile.open(archive, "r") as tar:
            for member in tar.getmembers():
                if member.size == 0:
                    continue
                if not member.isfile():
                    continue
                if any(fnmatch.fnmatch(member.name, pattern) for pattern in exclusions):
                    continue

                found = False
                with tar.extractfile(member) as f:
                    lineno = 0
                    try:
                        for line in f:
                            if header.search(line):
                                found = True
                                break
                            lineno += 1
                            if lineno >= 2:
                                break
                    except UnicodeDecodeError:
                        print(f"Cannot read {member.name} as text, skipping")
                        exitcode = 1

                if not found:
                    print(f"Missing copyright header in {member.name}")
                    exitcode = 1

    return exitcode


if __name__ == "__main__":
    sys.exit(main())
