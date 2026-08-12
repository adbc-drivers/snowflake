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

"""Generate workflows for a particular repository."""

import argparse
import functools
import json
import re
import subprocess
import sys
import typing
from pathlib import Path

import jinja2
import packaging.version
import tomlkit

from .generate import GenerateConfig


def write_workflow(
    root: Path, template, filename: str, params: dict[str, typing.Any]
) -> None:
    rendered = template.render(**params)
    rendered = rendered.splitlines()
    rendered = "\n".join(line.rstrip() for line in rendered)
    rendered += "\n"
    sink = root / filename
    with sink.open("w") as f:
        f.write(rendered)
        if not rendered.endswith("\n"):
            f.write("\n")
    print("Wrote", sink)


def generate_schema(args) -> int:
    """Generate JSON schema from Pydantic models."""
    schema = GenerateConfig.model_json_schema(mode="validation")
    output_path = Path(__file__).parent.parent / "schema" / "generate-schema.json"
    with output_path.open("w") as f:
        json.dump(schema, f, indent=2)
        f.write("\n")  # trailing newlines are nice
    print(f"Wrote schema to {output_path}")
    return 0


def template_not_implemented(message: str) -> str:
    raise NotImplementedError(message)


def generate_workflows(args) -> int:
    env = jinja2.Environment(
        loader=jinja2.PackageLoader("adbc_drivers_dev"),
        autoescape=jinja2.select_autoescape(),
        block_start_string="<%",
        block_end_string="%>",
        variable_start_string="<{",
        variable_end_string="}>",
        trim_blocks=True,
        undefined=jinja2.StrictUndefined,
    )
    env.globals["not_implemented"] = template_not_implemented

    config_path = args.repository / ".github/workflows/generate.toml"
    try:
        with config_path.open("rb") as f:
            params = tomlkit.load(f).unwrap()
    except FileNotFoundError:
        print(f"{config_path} not found.", file=sys.stderr)

        config_path.parent.mkdir(parents=True, exist_ok=True)
        with config_path.open("w") as f:
            # Add schema directive for tombi validation
            f.write(
                "#:schema https://raw.githubusercontent.com/adbc-drivers/dev/refs/heads/main/schema/generate-schema.json\n\n"
            )
            tomlkit.dump(
                GenerateConfig().model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
                f,
            )
        print("Wrote out defaults, please fill it in.", file=sys.stderr)

        return 1

    params = GenerateConfig.model_validate(params)
    print(params)
    workflows = args.repository / ".github/workflows"
    langs = {
        "go": ("Go", "go"),
        "rust": ("Rust", "rust"),
        "script": ("Custom", "src"),
    }

    retcode = 0
    for lang, (lang_human, lang_subdir_default) in langs.items():
        lang_config = params.lang.get(lang)
        if not lang_config:
            continue

        lang_subdir = (
            lang_config.subdir
            if lang_config.subdir is not None
            else lang_subdir_default
        )
        lang_tag_prefix = "" if lang_subdir == "." else f"{lang_subdir}/"
        lang_path_glob = "**" if lang_subdir == "." else f"{lang_subdir}/**"

        (args.repository / lang_subdir).mkdir(parents=True, exist_ok=True)

        lang_tools = {lang}
        lang_tools.update(lang_config.build.lang_tools)

        template = env.get_template("test.yaml")
        go_mod_path = (
            (lang_config.build.go_mod_path or lang_subdir)
            if lang == "go"
            else langs["go"][1]
        )
        lang_ctx = {
            "lang": lang,
            "lang_human": lang_human,
            "lang_subdir": lang_subdir,
            "lang_tag_prefix": lang_tag_prefix,
            "lang_path_glob": lang_path_glob,
            "go_mod_path": go_mod_path,
            "lang_config": lang_config,
            "lang_tools": lang_tools,
        }
        write_workflow(
            workflows,
            template,
            f"{lang}_test.yaml",
            {
                **params.to_dict(),
                **lang_ctx,
                "pull_request_trigger_paths": [f".github/workflows/{lang}_test.yaml"],
                "release": False,
                "workflow_name": "Test",
            },
        )
        write_workflow(
            workflows,
            template,
            f"{lang}_release.yaml",
            {
                **params.to_dict(),
                **lang_ctx,
                "release": True,
                "workflow_name": "Release",
            },
        )
        template = env.get_template("go_test_pr.yaml")
        if params._processed_secrets["all"]:
            write_workflow(
                workflows,
                template,
                f"{lang}_test_pr.yaml",
                {
                    **params.to_dict(),
                },
            )

        template = env.get_template("pixi.toml")
        write_workflow(
            args.repository / lang_subdir,
            template,
            "pixi.toml",
            {
                **params.to_dict(),
                **lang_ctx,
            },
        )

        license_template = args.repository / lang_subdir / "license.tpl"
        if not license_template.is_file():
            print(f"Missing {license_template}", file=sys.stderr)
            retcode = 1

        if lang == "go":
            template = env.get_template("golangci.toml")
            write_workflow(
                args.repository,
                template,
                ".golangci.toml",
                {
                    **params.to_dict(),
                },
            )

    for dev in ["dev.yaml", "dev_issues.yaml", "dev_pr.yaml"]:
        template = env.get_template(dev)
        write_workflow(
            workflows,
            template,
            dev,
            {
                **params.to_dict(),
                "go_mod_path": go_mod_path,
            },
        )

    return retcode


@functools.cache
def latest_action_version(action: str) -> (packaging.version.Version, str, str):
    # XXX: this won't work with repos that have multiple actions
    result = subprocess.check_output(
        [
            "git",
            "ls-remote",
            # don't use --refs which will hide the commit behind annotated tags
            "--tags",
            "--exit-code",
            "--quiet",
            f"https://github.com/{action}",
        ],
        text=True,
    )
    # do some remapping: we may have both refs/tags/v9 and refs/tags/v9^{}
    # when the remote uses annotated tags. we want to keep only the latter if
    # it exists

    # raw ref: sha
    refs = {}
    for line in result.strip().splitlines():
        sha, ref = line.split()
        tag = ref.removeprefix("refs/tags/").removesuffix("^{}")
        if tag == "master" or "-node" in tag or tag == "testEnableForGHES":
            # aws-actions/configure-aws-credentials, others have weird tags
            continue
        refs[ref] = (sha, tag)

    for ref in list(refs):
        if ref.endswith("^{}") and ref[:-3] in refs:
            del refs[ref[:-3]]

    tags = []
    for ref, (sha, tag) in refs.items():
        version = packaging.version.parse(tag.lstrip("v"))
        tags.append((version, tag, sha))

    tags.sort(key=lambda x: x[0])
    latest = tags[-1]
    return latest


def update_actions() -> None:
    root = Path(__file__).parent / "templates"
    templates = root.rglob("*.yaml")

    action_re = re.compile(r"uses: ([\w\-/]+)@([\w\-.]+)(\W*#.*)?")

    for template in templates:
        print("Updating", template)

        with template.open("r") as f:
            content = f.read()

            def replace_action(match: re.Match[str]) -> str:
                latest = latest_action_version(match.group(1))

                if match.group(2) == latest[2]:
                    print(f"  {match.group(1)} already at {latest[2]} ({latest[1]})")
                else:
                    print(
                        f"  {match.group(1)} updated from {match.group(2)} to {latest[2]} ({latest[1]})"
                    )
                return f"uses: {match.group(1)}@{latest[2]}  # {latest[1]}"

            new_content = action_re.sub(replace_action, content)

        with template.open("w") as f:
            f.write(new_content)


def main():
    parser = argparse.ArgumentParser()
    subcommand = parser.add_subparsers(dest="subcommand", required=True)

    generate = subcommand.add_parser("generate")
    generate.add_argument("repository", type=Path)

    subcommand.add_parser("generate-schema")
    subcommand.add_parser("update-actions")

    args = parser.parse_args()

    if args.subcommand == "generate":
        return generate_workflows(args)
    elif args.subcommand == "generate-schema":
        return generate_schema(args)
    elif args.subcommand == "update-actions":
        update_actions()
        return 0
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
