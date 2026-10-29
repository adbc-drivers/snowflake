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

from pathlib import Path

import pytest

from adbc_drivers_dev import make_checks, make_config


@pytest.mark.parametrize(
    ("driver", "driver_init"),
    [
        ("mysql", "AdbcDriverMysqlInit"),
        ("bigquery", "AdbcDriverBigqueryInit"),
        ("sap-hana", "AdbcDriverSapHanaInit"),
    ],
)
def test_check_required_symbols(driver: str, driver_init: str) -> None:
    config = make_config.MakeConfig(
        driver="rustdummy",
        lang=make_config.LangRust(lang="rust"),
    )
    make_checks.check_required_symbols(
        config,
        [
            "AdbcDatabaseNew",
            "AdbcConnectionInit",
            driver_init,
            "AdbcDriverInit",
            "bad_symbol",
        ],
        Path("driver.so"),
        driver,
    )

    with pytest.raises(RuntimeError, match="AdbcDriverInit"):
        make_checks.check_required_symbols(
            config,
            [
                "AdbcDatabaseNew",
                "AdbcConnectionInit",
                driver_init,
                "bad_symbol",
            ],
            Path("driver.so"),
            driver,
        )

    with pytest.raises(RuntimeError, match=driver_init):
        make_checks.check_required_symbols(
            config,
            ["AdbcDatabaseNew", "AdbcConnectionInit", "AdbcDriverInit", "bad_symbol"],
            Path("driver.so"),
            driver,
        )

    make_checks.check_required_symbols(
        make_config.MakeConfig(
            driver="rustdummy",
            lang=make_config.LangRust(lang="rust"),
            checks=make_config.CheckConfig(
                disable_driver_entrypoint_check=True,
            ),
        ),
        ["AdbcDatabaseNew", "AdbcConnectionInit", "AdbcDriverInit", "bad_symbol"],
        Path("driver.so"),
        driver,
    )


def test_check_disallowed_symbols() -> None:
    make_checks.check_disallowed_symbols(
        [],
        Path("driver.so"),
        "driver",
    )
    make_checks.check_disallowed_symbols(
        ["AdbcFooBar"],
        Path("driver.so"),
        "driver",
    )

    with pytest.raises(RuntimeError, match="bad_symbol"):
        make_checks.check_disallowed_symbols(
            ["AdbcFooBar", "bad_symbol"],
            Path("driver.so"),
            "driver",
        )


def test_extract_exported_linux_symbols() -> None:
    symbols = [
        "000000 T AdbcDriverInit",
        "000000 T AdbcDriverDriverInit",
        "         U external_symbol",
        "000000 B _cgo_runtime",
    ]
    exported_symbols = make_checks._extract_exported_linux_symbols(symbols)
    assert exported_symbols == ["AdbcDriverInit", "AdbcDriverDriverInit"]


def test_extract_linux_dependencies() -> None:
    dependencies = make_checks._extract_linux_dependencies(
        [
            "\tlinux-vdso.so.1 (0x00007fff)",
            "\tlibm.so.6 => /lib64/libm.so.6 (0x00007fff)",
            "\tlibmissing.so.1 => not found",
            "\t/lib64/ld-linux-x86-64.so.2 (0x00007fff)",
        ]
    )
    assert dependencies == {
        "linux-vdso.so.1",
        "libm.so.6",
        "libmissing.so.1",
        "/lib64/ld-linux-x86-64.so.2",
    }


def test_check_runtime_dependencies() -> None:
    output = [
        "linux-vdso.so.1 (0x00007fff)",
        "libgcc_s.so.1 => /lib64/libgcc_s.so.1 (0x00007fff)",
        "libm.so.6 => /lib64/libm.so.6 (0x00007fff)",
        "libpthread.so.0 => /lib64/libpthread.so.0 (0x00007fff)",
        "libc.so.6 => /lib64/libc.so.6 (0x00007fff)",
        "/lib64/ld-linux-x86-64.so.2 (0x00007fff)",
    ]
    allowed = make_checks._LINUX_RUNTIME_DEPENDENCIES | {
        make_checks._LINUX_LOADERS["amd64"]
    }
    make_checks.check_runtime_dependencies(
        make_checks._extract_linux_dependencies(output), Path("driver.so"), allowed
    )

    output.append("libfoobar.so => /opt/libfoobar.so (0x00007fff)")
    make_checks.check_runtime_dependencies(
        make_checks._extract_linux_dependencies(output),
        Path("driver.so"),
        allowed | {"libfoobar.so"},
    )

    with pytest.raises(RuntimeError, match="libfoobar.so"):
        make_checks.check_runtime_dependencies(
            make_checks._extract_linux_dependencies(output),
            Path("driver.so"),
            allowed,
        )

    with pytest.raises(RuntimeError, match="libmissing.so.1"):
        make_checks.check_runtime_dependencies(
            {"libmissing.so.1"},
            Path("driver.so"),
            allowed,
        )


def test_extract_exported_macos_symbols() -> None:
    symbols = [
        "000000 T _AdbcDriverMultiwordnameInit",
        "000000 U _external_symbol",
        "000000 D __cgo_runtime",
    ]
    exported = make_checks._extract_exported_macos_symbols(symbols)
    assert exported == ["AdbcDriverMultiwordnameInit"]


def test_extract_macos_dependencies() -> None:
    dependencies = make_checks._extract_macos_dependencies(
        [
            "driver.dylib:",
            "\tlibadbc_driver_godummy.dylib (compatibility version 0.0.0, current version 0.0.0)",
            "\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0, current version 1351.0.0)",
            "\t@rpath/libfoobar.dylib (compatibility version 0.0.0, current version 1.0.0)",
            "\t/System/Library/Frameworks/Security.framework/Versions/A/Security (compatibility version 1.0.0, current version 61123.0.0)",
        ]
    )
    assert dependencies == {
        "/usr/lib/libSystem.B.dylib",
        "@rpath/libfoobar.dylib",
        "/System/Library/Frameworks/Security.framework/Versions/A/Security",
    }

    dependencies = make_checks._extract_macos_dependencies(
        [
            "driver.dylib:",
            "",
            "\t/Users/runner/work/dev/dev/tests/make/rustdummy/target/release/deps/libadbc_dummy.dylib (compatibility version 0.0.0, current version 0.0.0)",
            "\t/usr/lib/libiconv.2.dylib (compatibility version 7.0.0, current version 7.0.0)",
        ]
    )
    assert dependencies == {"/usr/lib/libiconv.2.dylib"}


def test_check_macos_runtime_dependencies() -> None:
    output = [
        "driver.dylib:",
        "\tlibadbc_driver_godummy.dylib (compatibility version 0.0.0, current version 0.0.0)",
        "\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0, current version 1351.0.0)",
        "\t/usr/lib/libiconv.2.dylib (compatibility version 7.0.0, current version 7.0.0)",
        "\t/usr/lib/libresolv.9.dylib (compatibility version 1.0.0, current version 1.0.0)",
        "\t/System/Library/Frameworks/CoreFoundation.framework/Versions/A/CoreFoundation (compatibility version 150.0.0, current version 1953.1.0)",
        "\t/System/Library/Frameworks/CoreServices.framework/Versions/A/CoreServices (compatibility version 1.0.0, current version 1226.0.0)",
        "\t/System/Library/Frameworks/Security.framework/Versions/A/Security (compatibility version 1.0.0, current version 61123.0.0)",
        "\t/System/Library/Frameworks/SystemConfiguration.framework/Versions/A/SystemConfiguration (compatibility version 1.0.0, current version 1405.120.5)",
    ]
    make_checks.check_runtime_dependencies(
        make_checks._extract_macos_dependencies(output),
        Path("driver.dylib"),
        make_checks._MACOS_RUNTIME_DEPENDENCIES,
    )

    output.append(
        "\t@rpath/libfoobar.dylib (compatibility version 0.0.0, current version 1.0.0)"
    )
    make_checks.check_runtime_dependencies(
        make_checks._extract_macos_dependencies(output),
        Path("driver.dylib"),
        make_checks._MACOS_RUNTIME_DEPENDENCIES | {"@rpath/libfoobar.dylib"},
    )
    with pytest.raises(RuntimeError, match="@rpath/libfoobar.dylib"):
        make_checks.check_runtime_dependencies(
            make_checks._extract_macos_dependencies(output),
            Path("driver.dylib"),
            make_checks._MACOS_RUNTIME_DEPENDENCIES,
        )

    with pytest.raises(RuntimeError, match="/opt/lib/libSystem.B.dylib"):
        make_checks.check_runtime_dependencies(
            {"/opt/lib/libSystem.B.dylib"},
            Path("driver.dylib"),
            make_checks._MACOS_RUNTIME_DEPENDENCIES,
        )


def test_extract_windows_symbols() -> None:
    symbols = [
        "Dump of file driver.dll",
        "",
        "File Type: DLL",
        "",
        "    ordinal hint RVA      name",
        "",
        "          1    0 00001000 AdbcDriverInit",
        "          2    1 00001010 AdbcDriverMultiwordnameInit",
        "          3    2 00001020 bad_symbol = internal_symbol",
        "",
        "  Summary",
        "        1000 .data",
        "        6000 .text",
    ]
    exported = make_checks._extract_exported_windows_symbols(symbols)
    assert exported == ["AdbcDriverInit", "AdbcDriverMultiwordnameInit", "bad_symbol"]


def test_extract_windows_dependencies() -> None:
    dependencies = make_checks._extract_windows_dependencies(
        [
            "Microsoft (R) COFF/PE Dumper Version 14.44.35213.0",
            "",
            "Dump of file driver.dll",
            "",
            "File Type: DLL",
            "",
            "  Image has the following dependencies:",
            "",
            "    KERNEL32.dll",
            "    VCRUNTIME140.dll",
            "",
            "  Image has the following delay load dependencies:",
            "",
            "    foobar.dll",
            "",
            "  Summary",
            "",
            "        1000 .data",
        ]
    )
    assert dependencies == {"KERNEL32.DLL", "VCRUNTIME140.DLL", "FOOBAR.DLL"}


def test_check_windows_runtime_dependencies() -> None:
    make_checks.check_runtime_dependencies(
        make_checks._WINDOWS_RUNTIME_DEPENDENCIES,
        Path("driver.dll"),
        make_checks._WINDOWS_RUNTIME_DEPENDENCIES,
    )

    with pytest.raises(RuntimeError, match="FOOBAR.DLL"):
        make_checks.check_runtime_dependencies(
            {"KERNEL32.DLL", "FOOBAR.DLL"},
            Path("driver.dll"),
            make_checks._WINDOWS_RUNTIME_DEPENDENCIES,
        )


def test_check_linux_libc_requirement() -> None:
    make_checks.check_linux_libc_requirement(
        [
            " U function@GLIBC_2.17",
            " U function@GLIBCXX_3.4.19",
        ],
        "manylinux2014",
    )

    make_checks.check_linux_libc_requirement(
        [
            " U function@GLIBC_2.28",
            " U function@GLIBCXX_3.4.32",
        ],
        "manylinux_2_28",
    )

    with pytest.raises(RuntimeError, match="GLIBC_2.18"):
        make_checks.check_linux_libc_requirement(
            [" U function@GLIBC_2.18"], "manylinux2014"
        )

    with pytest.raises(RuntimeError, match="GLIBCXX_3.4.20"):
        make_checks.check_linux_libc_requirement(
            [" U function@GLIBCXX_3.4.20"], "manylinux2014"
        )

    with pytest.raises(RuntimeError, match="GLIBC_2.29"):
        make_checks.check_linux_libc_requirement(
            [" U function@GLIBC_2.29"], "manylinux_2_28"
        )

    with pytest.raises(RuntimeError, match="GLIBCXX_3.4.33"):
        make_checks.check_linux_libc_requirement(
            [" U function@GLIBCXX_3.4.33"], "manylinux_2_28"
        )

    with pytest.raises(ValueError, match="Unsupported manylinux policy"):
        make_checks.check_linux_libc_requirement([], "unknown")


def test_check_macos_rejects_new_deployment_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        make_checks.subprocess,
        "check_output",
        lambda *args, **kwargs: "      minos 14.0\n",
    )
    with pytest.raises(RuntimeError, match="macOS 14.0"):
        make_checks._check_macos_deployment_target(Path("driver.dylib"))


def test_check_macos_accepts_supported_deployment_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        make_checks.subprocess,
        "check_output",
        lambda *args, **kwargs: "      minos 13.0\n",
    )
    make_checks._check_macos_deployment_target(Path("driver.dylib"))
