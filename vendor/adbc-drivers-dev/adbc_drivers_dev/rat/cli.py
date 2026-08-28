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
import io
import re
import subprocess
import tarfile
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

import platformdirs
import requests

RAT_VERSION = "0.16.1"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path, help="Root directory of repository")

    args = parser.parse_args()
    root = args.root.resolve()
    print("Checking licenses for", root)

    # ------------------------------------------------------------
    # Download RAT if not present
    # ------------------------------------------------------------
    cache_dir = Path(
        platformdirs.user_cache_dir("adbc-drivers-dev", "ADBC Driver Foundry")
    )
    cache_dir.mkdir(parents=True, exist_ok=True)

    rat_path = cache_dir / f"apache-rat-{RAT_VERSION}.jar"
    if not rat_path.is_file():
        url = f"https://repo1.maven.org/maven2/org/apache/rat/apache-rat/{RAT_VERSION}/apache-rat-{RAT_VERSION}.jar"
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            try:
                with rat_path.open("wb") as sink:
                    for chunk in response.iter_content(chunk_size=8192):
                        sink.write(chunk)
            except Exception:
                rat_path.unlink(missing_ok=True)
                raise

    print("Using Apache RAT:", rat_path)

    # ------------------------------------------------------------
    # RAT does not respect .gitignore.  Create and check a tarball instead.
    # ------------------------------------------------------------
    commit = subprocess.check_output(
        ["git", "stash", "create"], cwd=root, text=True
    ).strip()
    if not commit:
        # No unstaged changes.
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip()

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
    # Load the file listing files imported from Apache repos
    # ------------------------------------------------------------
    apache_file = root / ".rat-apache"
    needs_apache_header = set()
    if apache_file.is_file():
        with apache_file.open("r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    needs_apache_header.add(line)

    with tempfile.TemporaryDirectory() as scratch:
        scratch = Path(scratch).resolve()
        archive = scratch / "rat.tar"
        subprocess.check_call(
            ["git", "archive", "--format=tgz", f"--output={archive}", commit], cwd=root
        )

        # ------------------------------------------------------------
        # Invoke RAT
        # ------------------------------------------------------------
        report = io.StringIO(
            subprocess.check_output(
                [
                    "java",
                    "-jar",
                    str(rat_path),
                    str(archive),
                    "-x",
                ],
                text=True,
            )
        )

        root = ET.parse(report).getroot()
        unapproved = 0
        for resource in root.findall("resource"):
            approvals = resource.findall("license-approval")
            if not approvals:
                continue
            if approvals[0].attrib["name"] == "true":
                continue

            filename = resource.attrib["name"]
            if any(fnmatch.fnmatch(filename, exclusion) for exclusion in exclusions):
                continue

            if unapproved == 0:
                print("Files without licenses or with unapproved licenses found:")
            unapproved += 1
            print("-", filename)

        # ------------------------------------------------------------
        # Check other aspects of the copyright header
        # ------------------------------------------------------------

        missing_copyright = []
        missing_apache_header = []
        # Has "This file has been modified..." header but is not listed in .rat-apache
        should_not_have_apache_header = []
        # Has "Licensed to the Apache Software Foundation" but is missing
        # "This file has been modified..."
        should_not_have_licensed_header = []
        copyright_re = re.compile(
            r"Copyright \(c\) [0-9]{4}(\s+[0-9]{4})? ADBC Drivers Contributors"
        )
        header_re = re.compile(
            r"This file has been modified from its original version, which is under the Apache License: Licensed to the Apache Software Foundation"
        )
        licensed_re = re.compile("Licensed to the Apache Software Foundation")
        sep_re = re.compile(r"[^a-zA-Z0-9,:()]+")
        with tarfile.open(archive, "r") as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue

                with tar.extractfile(member) as f:
                    lines = []
                    for _ in range(20):
                        lines.append(f.readline())
                try:
                    content = b" ".join(lines).decode("utf-8")
                except UnicodeDecodeError:
                    # decode will fail on non-text files so just use empty
                    # content so RAT will fail if the file isn't excluded
                    content = ""
                content = sep_re.sub(" ", content)

                if not copyright_re.search(content):
                    if (
                        not member.name.endswith("LICENSE.txt")
                        and not member.name.endswith("NOTICE.txt")
                        and not any(
                            fnmatch.fnmatch(member.name, exclusion)
                            for exclusion in exclusions
                        )
                    ):
                        missing_copyright.append(member.name)

                if member.name in needs_apache_header:
                    if not header_re.search(content):
                        missing_apache_header.append(member.name)
                elif header_re.search(content):
                    should_not_have_apache_header.append(member.name)
                elif licensed_re.search(content):
                    should_not_have_licensed_header.append(member.name)

        if missing_copyright:
            print("Files missing ADBC Drivers Contributors copyright header:")
            for name in missing_copyright:
                print("-", name)

        if missing_apache_header:
            print("Files missing 'This file has been modified' header:")
            for name in missing_apache_header:
                print("-", name)

        if should_not_have_apache_header:
            print("Files that should not have 'This file has been modified' header:")
            for name in should_not_have_apache_header:
                print("-", name)

        if should_not_have_licensed_header:
            print(
                "Files that have 'Licensed to the Apache Software Foundation' header,"
            )
            print("but are missing 'This file has been modified' header:")
            for name in should_not_have_licensed_header:
                print("-", name)

        unapproved += len(missing_copyright)
        unapproved += len(missing_apache_header)
        unapproved += len(should_not_have_apache_header)
        unapproved += len(should_not_have_licensed_header)

    return 1 if unapproved else 0
