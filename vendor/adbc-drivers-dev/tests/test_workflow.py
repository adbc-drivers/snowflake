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

import json
from pathlib import Path

import jinja2
import pytest
import tomlkit
from pydantic import ValidationError

from adbc_drivers_dev.generate import GenerateConfig, LangConfig, LangTestConfig


def test_model_default() -> None:
    config = GenerateConfig.model_validate({})
    assert config.driver == "(unknown)"
    assert config.private is False
    assert config.lang == {}
    assert config._processed_secrets == {
        "all": {},
        "build:release": {},
        "build:test": {},
        "test": {},
        "validate": {},
    }
    assert config._permissions == {}
    assert config.aws is None
    assert config.gcloud is False
    assert config.validation.extra_dependencies == {}
    assert config.validation.extra_pypi_dependencies == {}

    assert config.to_dict() == {
        "driver": "(unknown)",
        "repository": "(unknown)",
        "concurrency_key": None,
        "private": False,
        "lang": {},
        "secrets": {
            "all": {},
            "build:release": {},
            "build:test": {},
            "test": {},
            "validate": {},
        },
        "permissions": {},
        "aws": None,
        "gcloud": False,
        "azure": False,
        "validation": {
            "extra_dependencies": {},
            "extra_pypi_dependencies": {},
        },
    }

    assert config == GenerateConfig.model_validate({})


def test_model_custom() -> None:
    config = GenerateConfig.model_validate({"driver": "postgresql"})
    assert config.driver == "postgresql"
    assert config.to_dict()["driver"] == "postgresql"
    assert config == config
    assert config != GenerateConfig.model_validate({})

    config = GenerateConfig.model_validate({"private": True})
    assert config.private is True
    assert config.to_dict()["private"] is True
    assert config == config
    assert config != GenerateConfig.model_validate({})

    config = GenerateConfig.model_validate({"lang": {"python": True, "java": False}})
    assert config.lang == {"python": LangConfig(), "java": None}
    assert config == config
    assert config != GenerateConfig.model_validate({})


def test_github_stack_behavior_default() -> None:
    assert LangConfig().github_stack_behavior == "all"
    config = GenerateConfig.model_validate({"lang": {"go": True}})
    assert config.lang["go"].github_stack_behavior == "all"


@pytest.mark.parametrize("behavior", ["all", "bottom", "top"])
@pytest.mark.parametrize("key", ["github-stack-behavior", "github_stack_behavior"])
def test_github_stack_behavior(key: str, behavior: str) -> None:
    config = GenerateConfig.model_validate({"lang": {"go": {key: behavior}}})
    assert config.lang["go"].github_stack_behavior == behavior
    assert (
        config.model_dump(by_alias=True)["lang"]["go"]["github-stack-behavior"]
        == behavior
    )
    assert config.model_dump()["lang"]["go"]["github_stack_behavior"] == behavior


@pytest.mark.parametrize("behavior", ["invalid", "TOP", "", None, True, 1])
def test_github_stack_behavior_invalid(behavior) -> None:
    with pytest.raises(ValidationError):
        GenerateConfig.model_validate(
            {"lang": {"go": {"github-stack-behavior": behavior}}}
        )


def test_params_secrets() -> None:
    config = GenerateConfig.model_validate(
        {
            "secrets": {
                "foo": "bar",
                "spam": {"secret": "eggs"},
                "fizz": {"secret": "buzz", "contexts": ["test", "validate"]},
            }
        }
    )
    assert config._processed_secrets == {
        "all": {"foo": "bar", "spam": "eggs", "fizz": "buzz"},
        "build:release": {"foo": "bar", "spam": "eggs"},
        "build:test": {"foo": "bar", "spam": "eggs"},
        "test": {"foo": "bar", "spam": "eggs", "fizz": "buzz"},
        "validate": {"foo": "bar", "spam": "eggs", "fizz": "buzz"},
    }
    assert config.to_dict()["secrets"] == {
        "all": {"foo": "bar", "spam": "eggs", "fizz": "buzz"},
        "build:release": {"foo": "bar", "spam": "eggs"},
        "build:test": {"foo": "bar", "spam": "eggs"},
        "test": {"foo": "bar", "spam": "eggs", "fizz": "buzz"},
        "validate": {"foo": "bar", "spam": "eggs", "fizz": "buzz"},
    }


def test_params_aws() -> None:
    config = GenerateConfig.model_validate({"aws": {"region": "us-west-2"}})
    assert config.aws.region == "us-west-2"
    assert config._permissions == {"id_token": True}
    assert config.to_dict()["permissions"] == {"id_token": True}
    assert config._processed_secrets == {
        "all": {
            "AWS_ROLE": "AWS_ROLE",
            "AWS_ROLE_SESSION_NAME": "AWS_ROLE_SESSION_NAME",
        },
        "build:release": {},
        "build:test": {},
        "test": {},
        "validate": {},
    }
    assert config.to_dict()["secrets"] == {
        "all": {
            "AWS_ROLE": "AWS_ROLE",
            "AWS_ROLE_SESSION_NAME": "AWS_ROLE_SESSION_NAME",
        },
        "build:release": {},
        "build:test": {},
        "test": {},
        "validate": {},
    }


def test_params_gcloud() -> None:
    config = GenerateConfig.model_validate({"gcloud": True})
    assert config.gcloud is True
    assert config._permissions == {"id_token": True}
    assert config.to_dict()["permissions"] == {"id_token": True}
    assert config._processed_secrets == {
        "all": {
            "GCLOUD_SERVICE_ACCOUNT": "GCLOUD_SERVICE_ACCOUNT",
            "GCLOUD_WORKLOAD_IDENTITY_PROVIDER": "GCLOUD_WORKLOAD_IDENTITY_PROVIDER",
        },
        "build:release": {},
        "build:test": {},
        "test": {},
        "validate": {},
    }
    assert config.to_dict()["secrets"] == {
        "all": {
            "GCLOUD_SERVICE_ACCOUNT": "GCLOUD_SERVICE_ACCOUNT",
            "GCLOUD_WORKLOAD_IDENTITY_PROVIDER": "GCLOUD_WORKLOAD_IDENTITY_PROVIDER",
        },
        "build:release": {},
        "build:test": {},
        "test": {},
        "validate": {},
    }


def test_params_invalid() -> None:
    with pytest.raises(ValidationError):
        GenerateConfig.model_validate({"private": ""})

    with pytest.raises(ValidationError):
        GenerateConfig.model_validate({"environment": 2})

    with pytest.raises(ValidationError):
        GenerateConfig.model_validate({"environment": ""})

    with pytest.raises(ValidationError):
        GenerateConfig.model_validate(
            {
                "secrets": {
                    "fizz": {
                        "secret": "buzz",
                        "contexts": ["test", "validate"],
                        "foo": "bar",
                    },
                }
            }
        )

    with pytest.raises(ValidationError):
        GenerateConfig.model_validate(
            {
                "secrets": {
                    "fizz": {"secret": "buzz", "contexts": ["asdf"], "foo": "bar"},
                }
            }
        )


def test_model_unknown() -> None:
    with pytest.raises(ValidationError):
        GenerateConfig.model_validate({"unknown_key": "value"})

    with pytest.raises(ValidationError):
        GenerateConfig.model_validate({"aws": {"foo": "bar"}})

    with pytest.raises(ValidationError):
        GenerateConfig.model_validate({"aws": {"region": "foo", "foo": "bar"}})


def test_validation_config_alias() -> None:
    config = GenerateConfig.model_validate(
        {"validation": {"extra-dependencies": {"pytest": "^7.0", "black": "*"}}}
    )
    assert config.validation.extra_dependencies == {"pytest": "^7.0", "black": "*"}
    assert config.validation.extra_pypi_dependencies == {}

    config = GenerateConfig.model_validate(
        {"validation": {"extra_dependencies": {"mypy": "^1.0"}}}
    )
    assert config.validation.extra_dependencies == {"mypy": "^1.0"}
    assert config.validation.extra_pypi_dependencies == {}

    config = GenerateConfig.model_validate(
        {"validation": {"extra_pypi_dependencies": {"mypy": ">=1.0"}}}
    )
    assert config.validation.extra_dependencies == {}
    assert config.validation.extra_pypi_dependencies == {"mypy": ">=1.0"}


def test_params_aws_and_gcloud() -> None:
    config = GenerateConfig.model_validate(
        {"aws": {"region": "us-west-2"}, "gcloud": True}
    )
    assert config.aws.region == "us-west-2"
    assert config.gcloud is True
    assert config._permissions == {"id_token": True}

    # Ensure both sets of secrets end up in "all"
    assert config._processed_secrets["all"] == {
        "AWS_ROLE": "AWS_ROLE",
        "AWS_ROLE_SESSION_NAME": "AWS_ROLE_SESSION_NAME",
        "GCLOUD_SERVICE_ACCOUNT": "GCLOUD_SERVICE_ACCOUNT",
        "GCLOUD_WORKLOAD_IDENTITY_PROVIDER": "GCLOUD_WORKLOAD_IDENTITY_PROVIDER",
    }

    # Individual contexts should be empty too
    assert config._processed_secrets["build:release"] == {}
    assert config._processed_secrets["build:test"] == {}
    assert config._processed_secrets["test"] == {}
    assert config._processed_secrets["validate"] == {}


def test_default_model() -> None:
    config = GenerateConfig().model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )

    # Should include the basic fields with defaults
    assert config["driver"] == "(unknown)"
    assert config["private"] is False
    assert config["lang"] == {}
    assert config["secrets"] == {}
    assert config["gcloud"] is False
    assert config["validation"] == {
        "extra-dependencies": {},
        "extra-pypi-dependencies": {},
    }

    # Should not include None values (environment, aws)
    assert "environment" not in config
    assert "aws" not in config

    # Should not include private fields
    assert "_processed_secrets" not in config
    assert "_permissions" not in config


def test_repository_override() -> None:
    config = GenerateConfig.model_validate({"driver": "foo"})
    assert config.driver == "foo"
    assert config.repository == "foo"

    config = GenerateConfig.model_validate({"driver": "foo", "repository": "bar"})
    assert config.driver == "foo"
    assert config.repository == "bar"


def test_exclude_platforms_config() -> None:
    assert LangConfig().exclude_platforms == []
    for key in ("exclude-platforms", "exclude_platforms"):
        config = GenerateConfig.model_validate(
            tomlkit.loads(f'[lang.go]\n{key} = ["linux_arm64"]\n').unwrap()
        )
        assert config.lang["go"].exclude_platforms == ["linux_arm64"]
        assert config.lang["go"].model_dump(by_alias=True)["exclude-platforms"] == [
            "linux_arm64"
        ]


@pytest.mark.parametrize(
    "excluded",
    [
        ["linux_aarch64"],
        ["win_amd64"],
        "linux_arm64",
        ["linux_amd64", "linux_arm64", "macos_arm64", "windows_amd64"],
    ],
)
def test_exclude_platforms_invalid(excluded) -> None:
    with pytest.raises(ValidationError):
        LangConfig.model_validate({"exclude-platforms": excluded})


# LOCAL MODIFICATIONS: Snowflake Go and Rust must regenerate against the vendored tools.
@pytest.mark.parametrize(
    ("driver", "language", "dependency"),
    [
        (
            "snowflake",
            "go",
            'adbc-drivers-dev = { path = "../vendor/adbc-drivers-dev", editable = true }',
        ),
        (
            "snowflake",
            "rust",
            'adbc-drivers-dev = { path = "../vendor/adbc-drivers-dev", editable = true }',
        ),
        (
            "postgresql",
            "go",
            'adbc-drivers-dev = { git = "https://github.com/adbc-drivers/dev" }',
        ),
    ],
)
def test_pixi_dependency_source(driver: str, language: str, dependency: str) -> None:
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
    params = GenerateConfig(driver=driver).to_dict()
    params["lang"] = language
    rendered = env.get_template("pixi.toml").render(**params)

    assert dependency in rendered
    assert rendered.count("adbc-drivers-dev =") == 1


def test_generate_schema_matches_model() -> None:
    schema_path = Path(__file__).parents[1] / "schema" / "generate-schema.json"
    assert json.loads(schema_path.read_text()) == GenerateConfig.model_json_schema(
        mode="validation"
    )


def test_test_timeout_config() -> None:
    assert LangTestConfig().timeout_minutes == 15
    for key in ("timeout-minutes", "timeout_minutes"):
        config = LangTestConfig.model_validate({key: 30})
        assert config.timeout_minutes == 30
        assert config.model_dump(by_alias=True)["timeout-minutes"] == 30


@pytest.mark.parametrize("timeout", [0, -1, "15", "PT15M", 1.5, 15.0, True, None])
def test_test_timeout_invalid(timeout) -> None:
    with pytest.raises(ValidationError):
        LangTestConfig.model_validate({"timeout-minutes": timeout})
