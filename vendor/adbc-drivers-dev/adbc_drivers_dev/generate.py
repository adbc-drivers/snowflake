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

import typing

from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator

# Define workflow contexts in a single location
WorkflowContext = typing.Literal["build:test", "build:release", "test", "validate"]
WORKFLOW_CONTEXTS: tuple[WorkflowContext, ...] = (
    "build:test",
    "build:release",
    "test",
    "validate",
)


class SecretConfigDict(BaseModel):
    """Secret configuration with explicit secret name and contexts."""

    model_config = {"extra": "forbid"}

    secret: str = Field(
        description="The name of the GitHub secret to use for this secret variable"
    )
    contexts: list[WorkflowContext] = Field(
        default_factory=lambda: list(WORKFLOW_CONTEXTS),
        description="Workflow contexts where this secret should be available",
    )


class AwsConfig(BaseModel):
    """AWS authentication configuration."""

    model_config = {"extra": "forbid"}

    region: str = Field(
        description="AWS region to use for authentication (e.g., us-west-2, us-east-1)"
    )


class LangBuildConfig(BaseModel):
    # validate_by_{name,alias} are to let us map "additional-make-args" to "additional_make_args"
    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    additional_make_args: list[str] = Field(
        default_factory=list,
        alias="additional-make-args",
        description="A list of additional arguments to pass to adbc-make.",
    )

    go_mod_path: str | None = Field(
        default=None,
        alias="go-mod-path",
        description="Path containing the go.mod file for Go drivers. Used to cache dependencies in CI. Defaults to the language subdir.",
    )

    lang_tools: list[str] = Field(
        default_factory=list,
        alias="lang-tools",
        description="Install tools for these languages to use in the build.",
    )

    environment_contexts: list[str] = Field(
        default_factory=lambda: ["test", "validate"],
        alias="environment-contexts",
        description="What parts of the build require the environment.",
    )


class LangValidateSpec(BaseModel):
    """A single config to validate."""

    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    service_name: str = Field(
        default="test-service",
        description="docker-compose service to start",
    )
    vendor_version: str = Field(
        default="latest",
        description="version to pass to the validation suite",
    )


class LangValidationConfig(BaseModel):
    """Options for validation suite."""

    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    skip: bool = Field(
        default=False,
        description="Whether to skip the validation suite in CI (this should only be used temporarily while setting up a driver)",
    )
    configs: list[LangValidateSpec] = Field(
        default_factory=lambda: [LangValidateSpec()],
        description="A list of configurations to run the validation suite with. Each configuration will be run in a separate job",
    )


class LangTestConfig(BaseModel):
    """Options for test suite."""

    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    skip: bool = Field(
        default=False,
        description="Whether to skip tests in CI (this should only be used temporarily while setting up a driver or for build-only drivers)",
    )
    service_name: str = Field(
        alias="service-name",
        default="test-service",
        description="docker-compose service to start for tests",
    )


class LangConfig(BaseModel):
    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    build: LangBuildConfig = Field(
        default_factory=LangBuildConfig,
        description="Configuration for building the driver.",
    )
    subdir: str | None = Field(
        default=None,
        description="Override the default subdirectory for this language. Use '.' to place files at the repository root.",
    )
    test: LangTestConfig = Field(
        default_factory=LangTestConfig,
        description="Configuration for the driver test suite.",
    )
    validation: LangValidationConfig = Field(
        default_factory=LangValidationConfig,
        description="Configuration for the driver validation suite.",
    )

    @model_validator(mode="before")
    @classmethod
    def true_is_enabled(cls, data: typing.Any) -> typing.Any:
        if isinstance(data, bool):
            return {}
        return data


class ValidationConfig(BaseModel):
    """Configuration for validation workflows."""

    # validate_by_{name,alias} are to let us map "extra_dependencies" to "extra-dependencies"
    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    extra_dependencies: dict[str, typing.Any] = Field(
        default_factory=dict,
        alias="extra-dependencies",
        json_schema_extra={
            "description": """Additional dependencies to install for validation workflows. Specify as key-value pairs. Example:

[validation.extra-dependencies]
pytest = ">=7,<8"
black = "*\""""
        },
    )

    extra_pypi_dependencies: dict[str, typing.Any] = Field(
        default_factory=dict,
        alias="extra-pypi-dependencies",
        json_schema_extra={
            "description": """Additional dependencies to install for validation workflows. Specify as key-value pairs. Example:

[validation.extra-pypi-dependencies]
pytest = ">=7,<8"
black = "*\""""
        },
    )


class GenerateConfig(BaseModel):
    """Config for workflow generation."""

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Schema for generate.toml",
            "description": "You can validate your generate.toml against this schema with tools like tombi (https://tombi-toml.github.io/tombi/docs/linter). Requires placing a `#:schema` directive at the top of your generate.toml file.",
        },
    }

    repository: str | None = Field(
        default=None,
        description="GitHub repository name (if different from driver name)",
    )

    driver: str = Field(
        default="(unknown)",
        description="Driver name. Should be lowercase (e.g., postgresql, sqlite)",
    )
    environment: str | None = Field(
        default=None,
        description="Name of a GitHub Actions Environment to use when including secrets in workflows",
    )
    private: bool = Field(
        default=False,
        description="Whether the driver is private. Most drivers will be not be private so you can omit this.",
    )
    lang: dict[str, LangConfig | None] = Field(
        default_factory=dict,
        json_schema_extra={
            "description": """Programming language(s) to enable workflows for. Only go and rust are supported. Keys should be lowercase. Set to true to enable with default config, or false (the default) to disable. Example:

[lang]
go = true

[lang.rust.build]
additional-make-args = ["example"]"""
        },
    )
    secrets: dict[str, str | SecretConfigDict] = Field(
        default_factory=dict,
        json_schema_extra={
            "description": """Secrets to enable in workflows. By default, no secrets are available in your generated workflows unless you specify them here.

To make a secret available in all workflows, use the simple syntax:

[secrets]
MY_TOKEN = "GITHUB_SECRET_NAME"

If you want more fine-grained control, you can restrict secrets to specific workflows. To do this, specify contexts contexts ('build:release', 'test', 'validate') like this:

[secrets.DB_PASSWORD]
secret = "TEST_DB_SECRET"
contexts = ["test", "validate"]"""
        },
    )
    aws: AwsConfig | None = Field(
        default=None,
        json_schema_extra={
            "description": """Enables AWS authentication in workflows. Automatically adds AWS_ROLE and AWS_ROLE_SESSION_NAME secrets, and sets id_token permissions. Example:

[aws]
region = "us-west-2\""""
        },
    )
    gcloud: bool = Field(
        default=False,
        json_schema_extra={
            "description": """Enables Google Cloud authentication in workflows. Automatically adds GCLOUD_SERVICE_ACCOUNT and GCLOUD_WORKLOAD_IDENTITY_PROVIDER secrets, and sets id_token permissions. Set to true to enable. Example:

gcloud = true"""
        },
    )
    validation: ValidationConfig = Field(
        default_factory=ValidationConfig,
        json_schema_extra={
            "description": "Configuration for validation workflows. Currently supports specifying extra dependencies."
        },
    )

    # These fields are computed and not part of the input. PrivateAttr us
    # automatically omit them when we serialize
    _processed_secrets: dict[str, dict[str, str]] = PrivateAttr(default_factory=dict)
    _permissions: dict[str, bool] = PrivateAttr(default_factory=dict)

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str | None) -> str | None:
        if v is not None and not v.strip():
            raise ValueError("environment must be non-empty if provided")
        return v

    @field_validator("lang", mode="before")
    @classmethod
    def lang_boolean(cls, value: typing.Any) -> typing.Any:
        value = value or {}
        return {
            k: LangConfig() if v is True else None if v is False else v
            for k, v in value.items()
        }

    @model_validator(mode="after")
    def default_repository(self) -> "GenerateConfig":
        if self.repository is None:
            self.repository = self.driver
        return self

    @model_validator(mode="after")
    def process_secrets_and_permissions(self) -> "GenerateConfig":
        """Process secrets configuration and set up permissions."""
        self._processed_secrets = {context: {} for context in WORKFLOW_CONTEXTS}

        # Process each secret configuration
        for secret_var, secret_config in self.secrets.items():
            if isinstance(secret_config, str):
                # Simple string format: apply to all contexts
                for context in self._processed_secrets:
                    self._processed_secrets[context][secret_var] = secret_config
            else:
                # Dict format with explicit contexts
                for context in secret_config.contexts:
                    self._processed_secrets[context][secret_var] = secret_config.secret

        # Build the "all" context with all unique secrets
        all_secrets = {}
        for context_secrets in self._processed_secrets.values():
            all_secrets.update(context_secrets)

        if self.aws:
            all_secrets["AWS_ROLE"] = "AWS_ROLE"
            all_secrets["AWS_ROLE_SESSION_NAME"] = "AWS_ROLE_SESSION_NAME"

        if self.gcloud:
            all_secrets["GCLOUD_SERVICE_ACCOUNT"] = "GCLOUD_SERVICE_ACCOUNT"
            all_secrets["GCLOUD_WORKLOAD_IDENTITY_PROVIDER"] = (
                "GCLOUD_WORKLOAD_IDENTITY_PROVIDER"
            )

        if self.private:
            all_secrets["COLUMNAR_CLOUD_API_TOKEN"] = "COLUMNAR_CLOUD_API_TOKEN"

        # No need to explicitly pass this (also GitHub will complain if you try to)
        all_secrets = {k: v for k, v in all_secrets.items() if v != "GITHUB_TOKEN"}

        self._processed_secrets["all"] = all_secrets

        # Set permissions
        if self.aws or self.gcloud:
            self._permissions["id_token"] = True

        return self

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            "driver": self.driver,
            "repository": self.repository,
            "environment": self.environment,
            "private": self.private,
            "lang": self.lang,
            "secrets": self._processed_secrets,
            "permissions": self._permissions,
            "aws": self.aws.model_dump() if self.aws else None,
            "gcloud": self.gcloud,
            "validation": {
                "extra_dependencies": self.validation.extra_dependencies,
                "extra_pypi_dependencies": self.validation.extra_pypi_dependencies,
            },
        }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GenerateConfig):
            return NotImplemented
        return self.to_dict() == other.to_dict()
