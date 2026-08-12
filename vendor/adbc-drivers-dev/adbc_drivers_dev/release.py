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
import subprocess
import tempfile
from pathlib import Path

import packaging.version
import pygit2

from . import changelog


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path, help="The path to the driver root")
    parser.add_argument("tag", type=str, help="The tag to release")
    parser.add_argument(
        "--dry-run", action="store_true", help="Do not actually release"
    )
    args = parser.parse_args()

    root = args.root
    if "/" in args.tag:
        subdir, _, version = args.tag.partition("/")
        version = packaging.version.parse(version)
        prefix = f"{subdir}/v"
    else:
        subdir = "."
        version = packaging.version.parse(args.tag)
        prefix = "v"

    print("Subdir:", subdir)
    print("Version:", version)

    # Figure out the previous tag so we can compute the changelog.
    # Note that we don't use the Go semver, and we assume that there are no
    # dev/other modifiers (which would be invalid here)
    repo = pygit2.Repository(root)
    tags = []
    for ref in repo.references:
        if ref.startswith(f"refs/tags/{prefix}"):
            tag = ref[len("refs/tags/") :]
            v = packaging.version.parse(tag[len(prefix) :])
            if v.is_prerelease or v.is_devrelease:
                continue
            tags.append((tag, v))

    tags.sort(key=lambda v: v[1])
    tags = list(filter(lambda v: v[1] < version, tags))

    if not tags:
        # No previous version
        previous_tag = None
    else:
        previous_tag = tags[-1][0]

    print("Previous tag:", previous_tag, f"(found: {', '.join(t[0] for t in tags)})")

    title, log = changelog.generate_changelog(
        root, subdir, version, previous_tag, args.tag
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".md") as f:
        print("#", title)
        print()
        print(log)

        f.write(log)
        f.flush()

        command = [
            "gh",
            "release",
            "create",
            args.tag,
            "--draft",
            "--title",
            title,
            "--verify-tag",
            "--notes-file",
            f.name,
        ]
        print("*", " ".join(command))
        if args.dry_run:
            print("Dry run, not actually releasing")
        else:
            subprocess.check_call(command)


if __name__ == "__main__":
    main()
