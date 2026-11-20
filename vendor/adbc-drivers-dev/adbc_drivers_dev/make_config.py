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

import os
import secrets
import shlex
import subprocess
import sys
import time
import tomllib
import typing
from pathlib import Path

from pydantic import BaseModel, Field

_GO_VERSION_FLAG = "github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion"
_SMUGGLE_VARS = {"CGO_CFLAGS", "CGO_LDFLAGS", "GOWORK", "PROTOC"}
_APPEND_ENV_VARS = {"CGO_CFLAGS", "CGO_LDFLAGS"}


def merge_build_env(
    base: typing.Mapping[str, str], overrides: typing.Mapping[str, str]
) -> dict[str, str]:
    """Merge build variables, preserving user-supplied CGO flags."""
    env = dict(base)
    for key, value in overrides.items():
        if key in _APPEND_ENV_VARS and env.get(key):
            env[key] += " " + value
        else:
            env[key] = value
    return env


class MakeEnv(BaseModel):
    ci: bool = Field(
        default=False, description="Whether to build the driver in CI mode"
    )
    debug: bool = Field(
        default=False, description="Whether to build the driver in debug mode"
    )
    host_platform: typing.Literal["linux", "macos", "windows"]
    host_architecture: typing.Literal["amd64", "arm64"]
    target_platform: typing.Literal["linux", "macos", "windows"]
    target_architecture: typing.Literal["amd64", "arm64"]
    repo_root: Path
    driver_root: Path
    version: str

    @property
    def shared_library_affix(self) -> tuple[str, str]:
        """Prefix/suffix for name of library to generate."""
        if self.target_platform == "linux":
            return ("lib", ".so")
        elif self.target_platform == "macos":
            return ("lib", ".dylib")
        elif self.target_platform == "windows":
            # For CI, we always prefix the final artifact with "lib", though
            # that's not typical on Windows. Hence source_library_affix below.
            return ("lib", ".dll")
        else:
            raise ValueError(f"Unknown target platform: {self.target_platform}")

    @property
    def source_library_affix(self) -> tuple[str, str]:
        """Prefix/suffix for name of library to copy from."""
        if self.target_platform == "windows":
            return ("", ".dll")
        return self.shared_library_affix

    def shared_library_name(self, driver: str) -> str:
        prefix, suffix = self.shared_library_affix
        output_name = f"{prefix}adbc_driver_{driver}{suffix}"
        return output_name

    @property
    def is_cross_compile(self) -> bool:
        return (
            self.host_platform != self.target_platform
            or self.host_architecture != self.target_architecture
        )

    @property
    def use_docker(self) -> bool:
        if self.target_platform == "linux":
            return self.is_cross_compile or (not self.debug and self.ci)
        return False


class MakePlan(BaseModel):
    make_env: MakeEnv
    make_config: "MakeConfig"
    env_vars: dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables to set when building the driver",
    )
    commands: list[list[str]] = Field(
        default_factory=list, description="The commands to run to build the driver"
    )
    pre_commands: list[list[str]] = Field(
        default_factory=list,
        description="Commands to run directly on the host before building the driver",
    )
    cleanup_paths: list[Path] = Field(
        default_factory=list,
        description="Generated files to remove after building the driver",
    )
    artifact_path: Path | None
    docker_container: str | None

    @property
    def target_path(self) -> Path:
        output_dir = self.make_env.driver_root / "build"
        output_name = self.make_env.shared_library_name(self.make_config.driver)
        return output_dir / output_name

    def _run_direct(self, commands: list[list[str]]) -> None:
        for command in commands:
            print(
                "*",
                " ".join(shlex.quote(arg) for arg in command),
                f"[{self.make_env.driver_root}]",
                file=sys.stderr,
            )
            subprocess.run(
                command,
                cwd=self.make_env.driver_root,
                env=merge_build_env(os.environ, self.env_vars),
                check=True,
            )

    def _docker_outer_env(self) -> dict[str, str]:
        return {
            **os.environ,
            "SOURCE_ROOT": str(self.make_env.repo_root),
            "ARCH": self.make_env.target_architecture,
            "DOCKER_DEFAULT_PLATFORM": f"{self.make_env.target_platform}/{self.make_env.target_architecture}",
            "MANYLINUX": self.make_config.manylinux,
        }

    def _run_docker(self) -> None:
        outer_env = self._docker_outer_env()
        build_env = {
            key: value for key, value in os.environ.items() if key in _SMUGGLE_VARS
        }
        build_env = merge_build_env(build_env, self.env_vars)
        # LOCAL MODIFICATIONS: manylinux builds run as the host UID; set writable
        # caches in the actual docker exec shell, not only the Compose environment.
        if self.docker_container == "manylinux":
            build_env.update(
                GOCACHE="/tmp/go-build",
                GOMODCACHE="/tmp/go-mod",
                GOPATH="/tmp/go",
            )
        elif self.docker_container == "manylinux-rust":
            build_env.update(
                HOME="/tmp",
                XDG_CACHE_HOME="/tmp/.cache",
                CARGO_HOME="/.cargo",
            )
        inner_env = ["env"]
        inner_env += [f"{key}={shlex.quote(value)}" for key, value in build_env.items()]

        user_args = []
        if hasattr(os, "getuid"):
            user_args = ["--user", str(os.getuid())]

        container_name = f"adbc-make-{self.make_config.driver}-{secrets.token_hex(4)}"

        # pull now, so it's not included in startup time below
        try:
            subprocess.check_call(
                [
                    "docker",
                    "compose",
                    "pull",
                    self.docker_container,
                ],
                env=outer_env,
                cwd=Path(__file__).parent,
            )
        except subprocess.CalledProcessError:
            # Couldn't pull, so try to build
            subprocess.check_call(
                [
                    "docker",
                    "compose",
                    "build",
                    self.docker_container,
                ],
                env=outer_env,
                cwd=Path(__file__).parent,
            )

        with subprocess.Popen(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "--name",
                container_name,
                *user_args,
                *(
                    arg
                    for volume in self.make_config.additional_volumes
                    for arg in ("-v", volume)
                ),
                self.docker_container,
                "bash",
                "-c",
                "sleep infinity",
            ],
            env=outer_env,
            cwd=Path(__file__).parent,
        ) as proc:
            try:
                # Wait for container to initialize
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    try:
                        subprocess.check_call(
                            [
                                "docker",
                                "exec",
                                *user_args,
                                container_name,
                                "true",
                            ],
                            env=outer_env,
                        )
                        break
                    except subprocess.CalledProcessError:
                        time.sleep(1)

                workdir = f"/source/{self.make_env.driver_root.relative_to(self.make_env.repo_root)}"
                for command in self.commands:
                    if proc.poll() is not None:
                        raise RuntimeError(
                            f"Docker container {container_name} exited unexpectedly"
                        )
                    wrapped_command = [
                        "docker",
                        "exec",
                        *user_args,
                        "--workdir",
                        workdir,
                        container_name,
                        "bash",
                        "-c",
                        " ".join(inner_env + [shlex.quote(arg) for arg in command]),
                    ]
                    print(
                        "*", " ".join(wrapped_command), f"[{workdir}]", file=sys.stderr
                    )
                    subprocess.run(
                        wrapped_command,
                        cwd=self.make_env.driver_root,
                        env=outer_env,
                        check=True,
                    )
            finally:
                # result ignored
                subprocess.run(["docker", "kill", container_name], env=outer_env)
                proc.terminate()
                proc.wait(timeout=30)

    def run(self) -> None:
        target_path = self.target_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if self.artifact_path is not None:
            self.artifact_path.parent.mkdir(parents=True, exist_ok=True)
        self._run_direct(self.pre_commands)
        if self.docker_container is not None:
            self._run_docker()
        else:
            self._run_direct(self.commands)
        if self.artifact_path is not None:
            self.artifact_path.resolve().copy(self.target_path)
        self.target_path.chmod(0o755)
        for path in self.cleanup_paths:
            path.unlink(missing_ok=True)


class LangGo(BaseModel):
    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    lang: typing.Literal["go"]
    build_tags: list[str] = Field(default_factory=list, alias="build-tags")
    release_build_tags: list[str] = Field(
        default_factory=list, alias="release-build-tags"
    )
    go_mod_path: str | None = Field(default=None, alias="go-mod-path")


class LangRust(BaseModel):
    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    lang: typing.Literal["rust"]
    features: typing.List[str] = Field(
        default_factory=list,
        description="The features to enable when building the Rust driver, e.g. ['static-linking', 'bundled']",
    )
    manifest_path: str | None = Field(
        default=None,
        alias="manifest-path",
    )


class LangScript(BaseModel):
    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    lang: typing.Literal["script"]
    toolchain: typing.Literal["cpp", "go", "rust"]

    @property
    def docker_container(self) -> str:
        if self.toolchain == "cpp":
            return "manylinux-cpp"
        elif self.toolchain == "go":
            return "manylinux"
        elif self.toolchain == "rust":
            return "manylinux-rust"
        else:
            raise ValueError(f"Unknown toolchain: {self.toolchain}")


class CheckConfig(BaseModel):
    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    disable_driver_entrypoint_check: bool = Field(
        default=False,
        alias="disable-driver-entrypoint-check",
        description="Disable the check for the symbol AdbcDriverFoobarInit",
    )


class MakeConfig(BaseModel):
    model_config = {
        "extra": "forbid",
        "validate_by_name": True,
        "validate_by_alias": True,
    }

    driver: str = Field(description="The driver to build, e.g. 'spark', 'datafusion'")
    lang: typing.Union[LangGo | LangRust | LangScript] = Field(
        discriminator="lang", description="The implementation language"
    )
    manylinux: str = Field(
        default="manylinux2014",
        description="The manylinux version to use when verifying allowed symbols on Linux, e.g. 'manylinux2014', 'manylinux_2_28'",
    )
    additional_volumes: list[str] = Field(
        default_factory=list,
        alias="additional-volumes",
        description="Additional Docker volume mounts, in HOST:CONTAINER format",
    )
    additional_runtime_dependencies: dict[
        typing.Literal["linux", "macos", "windows"], list[str]
    ] = Field(
        default_factory=dict,
        alias="additional-runtime-dependencies",
        description="Additional runtime dependencies allowed by platform",
    )
    checks: CheckConfig = Field(
        default_factory=CheckConfig,
        description="Checks to perform on the built driver",
    )

    def build_plan(self, config: MakeEnv) -> MakePlan:
        env_vars = default_build_env(config)

        if isinstance(self.lang, LangGo):
            module_path = Path(self.lang.go_mod_path or ".")
            module_root = config.driver_root / module_path
            ldflags = [
                # Don't exclude symbols so panics will have symbol information
                # "-s",
                # Exclude DWARF debug tables
                "-w",
                # Embed Go version
                f"-X {_GO_VERSION_FLAG}={config.version}",
            ]
            # Include IANA timezone data when the host has no timezone database.
            tags = ["driverlib", "timetzdata"]
            if config.debug:
                tags.append("assert")
            tags.extend(self.lang.build_tags)
            if not config.debug:
                tags.extend(self.lang.release_build_tags)

            go = ["go"]
            if self.lang.go_mod_path:
                go.extend(["-C", str(module_path)])

            output_name = config.shared_library_name(self.driver)
            module_output = module_root / "build" / output_name
            args = [
                *go,
                "build",
                "-buildmode=c-shared",
                f"-tags={','.join(tags)}",
            ]

            pre_commands = []
            docker_container = None
            if config.target_platform == "macos":
                ldflags.extend(
                    [
                        "-linkmode external",
                        "-extldflags=-Wl,-exported_symbol,_Adbc*",
                    ]
                )
            elif config.use_docker:
                env_vars["GOWORK"] = "off"
                pre_commands.append([*go, "mod", "vendor"])
                ldflags.extend(
                    [
                        "-linkmode external",
                        "-extldflags=-Wl,--version-script=/only-export-adbc.ld",
                    ]
                )
                docker_container = "manylinux"

            args.extend(
                [
                    f"-ldflags={' '.join(ldflags)}",
                    "-o",
                    (Path("build") / output_name).as_posix(),
                    "./pkg",
                ]
            )

            artifact_path = None
            if module_root != config.driver_root:
                artifact_path = module_output

            return MakePlan(
                make_env=config,
                make_config=self,
                env_vars=env_vars,
                commands=[args],
                pre_commands=pre_commands,
                cleanup_paths=[module_output.with_suffix(".h")],
                artifact_path=artifact_path,
                docker_container=docker_container,
            )

        elif isinstance(self.lang, LangRust):
            args = ["cargo", "build"]

            artifact_path = config.driver_root
            manifest_path = config.driver_root / "Cargo.toml"
            if self.lang.manifest_path:
                artifact_path /= self.lang.manifest_path
                manifest_path = artifact_path / "Cargo.toml"
                args.append("--manifest-path")
                # Use relative path so it also works in Docker
                args.append(str(Path(self.lang.manifest_path) / "Cargo.toml"))

            artifact_path /= "target"
            if config.debug:
                artifact_path /= "debug"
            else:
                args.append("--release")
                artifact_path /= "release"

            if self.lang.features:
                args.append("--features")
                args.append(",".join(self.lang.features))

            with manifest_path.open("rb") as f:
                cargo_toml = tomllib.load(f)

            if "lib" in cargo_toml and "name" in cargo_toml["lib"]:
                lib_name = cargo_toml["lib"]["name"]
            else:
                lib_name = cargo_toml["package"]["name"].replace("-", "_")

            prefix, suffix = config.source_library_affix
            artifact_path /= f"{prefix}{lib_name}{suffix}"

            docker_container = None
            if config.use_docker:
                docker_container = "manylinux-rust"

            return MakePlan(
                make_env=config,
                make_config=self,
                env_vars=env_vars,
                commands=[args],
                artifact_path=artifact_path,
                docker_container=docker_container,
            )

        elif isinstance(self.lang, LangScript):
            args = ["./ci/scripts/build.sh"]
            if config.debug:
                args.append("test")
            else:
                args.append("release")

            args.append(config.target_platform)
            args.append(config.target_architecture)

            if config.target_platform == "windows" and config.ci:
                # Force use of Git Bash on GitHub Actions
                args = [r"C:\Program Files\Git\bin\bash.EXE", *args]

            docker_container = None
            if config.use_docker:
                docker_container = self.lang.docker_container

            return MakePlan(
                make_env=config,
                make_config=self,
                env_vars=env_vars,
                commands=[args],
                artifact_path=None,
                docker_container=docker_container,
            )

        raise NotImplementedError(
            f"Build plan not implemented for lang={self.lang.lang}"
        )


def default_build_env(config: MakeEnv) -> dict[str, str]:
    env = {}
    if config.target_platform == "macos":
        # We target macOS 13 (Ventura) because Go 1.27 requires that as a baseline
        # - https://clang.llvm.org/docs/ClangCommandLineReference.html#cmdoption-clang-mmacos-version-min
        # - https://doc.rust-lang.org/nightly/rustc/platform-support/apple-darwin.html#os-version
        env["MACOSX_DEPLOYMENT_TARGET"] = "13.0"
        env["CGO_CFLAGS"] = "-mmacos-version-min=13.0"
        env["CGO_LDFLAGS"] = "-mmacos-version-min=13.0"

    return env
