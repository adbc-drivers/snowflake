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

import collections
import datetime
from pathlib import Path, PurePosixPath

import pygit2
import tomlkit

from . import title_check


def parse_manifest(repo: pygit2.Repository, path: str, tree: pygit2.Tree) -> str:
    name = f"{path}/manifest.toml"
    name = str(PurePosixPath(name))
    diff = tree.diff_to_tree()
    for patch in diff:
        if patch.delta.old_file.path == name:
            blob = repo[patch.delta.old_file.id]
            manifest = tomlkit.loads(blob.data)
            driver_name = manifest.get("name")
            return driver_name

    raise FileNotFoundError(f"{name} not found in tree {tree.id}")


def generate_changelog(
    root: Path, subpath: str, version: str, start_ref: str | None, end_ref: str
) -> (str, str):
    repo = pygit2.Repository(root)

    end_commit, _ = repo.resolve_refish(end_ref)
    if start_ref is None:
        start_commit = None
    else:
        start_commit, _ = repo.resolve_refish(start_ref)

    driver_name = parse_manifest(repo, subpath, end_commit.tree)

    # Parse conventional commit metadata and build a curated changelog
    changelog = collections.defaultdict(list)
    # Track all commits affecting the subdirectory
    all_commits = []

    walker = repo.walk(end_commit.id, pygit2.GIT_SORT_TIME)
    if start_commit:
        walker.hide(start_commit.id)

    pred = title_check.any_components()
    for commit in walker:
        # If the driver lives in the repo root, include all commits.  Else,
        # scope down to commits that touch the driver subpath.
        include = subpath == "."

        if commit.parents:
            prev = commit.parents[0]
            diff = prev.tree.diff_to_tree(commit.tree)
            for patch in diff:
                include = include or patch.delta.new_file.path.startswith(subpath)
                include = include or patch.delta.old_file.path.startswith(subpath)
                if include:
                    break
        else:
            for blob in commit.tree:
                include = include or blob.name.startswith(subpath)
                if include:
                    break

        message = commit.message.splitlines()[0]
        if include:
            all_commits.append((commit.short_id, message))

            parsed = title_check.matches_commit_format(pred, message)
            changelog[parsed.category].append(parsed)

    date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    title = f"{driver_name} {version} ({date})"

    lines = []
    for category, heading in (
        ("feat", "New Features"),
        ("fix", "Bug Fixes"),
        ("perf", "Performance Improvements"),
        ("docs", "Documentation Updates"),
    ):
        if category in changelog and changelog[category]:
            lines.append(f"## {heading}")
            lines.append("")
            for commit in changelog[category]:
                lines.append(f"- {commit.subject}")
            lines.append("")

    lines.append("## Detailed Changelog")
    lines.append("")
    for commit in all_commits:
        lines.append(f"- {commit[0]}: {commit[1]}")

    rendered = "\n".join(lines).strip()
    return title, rendered
