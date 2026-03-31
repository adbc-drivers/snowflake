<!--
  Copyright (c) 2026 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Snowflake and sf_core Limitations in the Rust ADBC Driver

During the implementation and validation of the Rust ADBC Snowflake driver, we encountered several architectural limitations in both the Snowflake backend and the `sf_core` library. The driver currently employs workarounds for these issues.

This document serves as a reference for future improvements that should ideally be made upstream in `sf_core` or Snowflake itself.

## 1. Arrow IPC Format and `client_app_id`
**Issue:** Snowflake's backend determines the default `queryResultFormat` (Arrow IPC vs JSON rowset) based on the `CLIENT_APP_ID` field sent during the login request. Unrecognized client IDs fall back to JSON rowset.
**Impact:** JSON rowset format truncates `FLOAT`/`REAL` values to ~10 significant digits (losing IEEE 754 double precision) and converts `DBL_MAX` to `inf`.
**Workaround in Driver:** The driver currently omits overriding `client_app_id` (leaving it as `sf_core`'s default `"PythonConnector"`). This tricks Snowflake into recognizing the client and returning true Arrow IPC payloads with full-precision floats.
**Ideal Fix:**
- **Snowflake backend:** Honor `ALTER SESSION SET QUERY_RESULT_FORMAT = 'ARROW_FORCE'` uniformly, regardless of `CLIENT_APP_ID`.
- **sf_core:** Expose a native way to request Arrow format reliably without depending on a specific `CLIENT_APP_ID`, or ensure that custom client IDs can explicitly opt into Arrow format.

## 2. Float Bind Parameter Precision
**Issue:** When binding `Float64` parameters, Snowflake relies on string representation if using standard substitution. The Go driver (`gosnowflake`) formats float64 bind parameters with `FormatFloat(..., 32)` (float32 precision, ~7 significant digits), permanently truncating `3.141592653589793` to `"3.1415927"`.
**Impact:** Test expectations written for the Go driver expect truncated bind values.
**Workaround in Driver:** The Rust driver uses `format!("{v:?}")` for `Float64` (preserving full precision). For `Float32`, it avoids casting to `f64` first to prevent precision expansion (e.g. `3.14159` → `3.141590118408203`), preserving exact float32 semantics.
**Ideal Fix:**
- **Go Driver / gosnowflake:** Stop truncating float64 bind parameters to 32-bit precision.
- **sf_core:** Implement native Arrow batch binding (via the Arrow IPC streaming endpoint) rather than relying on SQL string substitution for bind parameters.

## 3. Timestamp Decoding (Epoch + Fraction)
**Issue:** `sf_core` decodes `TIMESTAMP_NTZ`/`TIMESTAMP_TZ` columns as a struct containing `epoch` (seconds, Int64) and `fraction` (nanoseconds, Int32/Int64). For pre-1970 timestamps (negative epochs), Snowflake uses floor division (e.g., `-9223372037` seconds + `145224192` nanoseconds), meaning the fraction is always positive.
**Impact:** Calculating total nanoseconds requires careful sign handling. If the fraction is subtracted when the epoch is negative (standard C-style truncation logic), the resulting date is off by ~1 second.
**Workaround in Driver:** The `ConvertingReader` must explicitly add the fraction regardless of the epoch's sign: `epoch * 1_000_000_000 + fraction * frac_to_ns`.
**Ideal Fix:**
- **sf_core:** When requested, natively map `TIMESTAMP_*` columns directly to Arrow `Timestamp` primitive arrays in the returned FFI stream, hiding the epoch/fraction struct complexity from the consumer.

## 4. Scaled Integers (`FIXED` type)
**Issue:** `sf_core` returns `FIXED` (e.g., `NUMBER(10,3)`) columns as raw `Int64` buffers representing the scaled integer (e.g., `12345` for `12.345`). The Arrow FFI stream schema only reports `Int64` with custom metadata (`logicalType: "FIXED"`, `scale: "3"`).
**Impact:** The ADBC driver has to implement a complex `ConvertingReader` to interpret this metadata and manually cast `Int64` to `Float64` (dividing by `10^scale`) or `Decimal128`.
**Workaround in Driver:** We intercept the Arrow FFI stream, read the metadata, and perform a secondary pass using `arrow_cast::cast` + manual division (for `Float64`) or unchecked precision reinterpretation via `build_unchecked` (values that exceed `target_type`'s precision are not rejected) for `Decimal128`.
**Ideal Fix:**
- **sf_core:** Automatically apply the scale. If `sf_core` detects a `FIXED` type with `scale > 0`, it should export the FFI stream as `Decimal128` natively. Arrow already supports `Decimal128Type`, which perfectly represents Snowflake's scaled integers without needing consumer-side interpretation.

## 5. FFI Error Path Memory Leak
**Issue:** If `ArrowArrayStreamReader::from_raw` fails on an FFI stream exported by `sf_core`, the Arrow C Data Interface specification states that the release callback is *not* called.
**Impact:** If the driver doesn't handle the error, the `Box::into_raw` pointer leaks.
**Workaround in Driver:** We catch the error in `map_err`, reconstruct the `Box` using `Box::from_raw`, and explicitly `drop` it.
**Ideal Fix:**
- **Arrow Crate:** Provide a safer abstraction for FFI stream consumption that guarantees cleanup on failure.

## 6. Timezone Handling in Arrow IPC
**Issue:** Snowflake's Arrow IPC responses include named timezones like `"UTC"` in the metadata. The Rust `arrow` crate requires the `"chrono-tz"` feature to parse these named timezones; otherwise, decoding fails.
**Impact:** Consumers of the ADBC driver (like `pyarrow`) receive invalid timezone errors.
**Workaround in Driver:** The driver enables the `"chrono-tz"` feature on its `arrow-array` dependency.
**Ideal Fix:**
- **sf_core:** Normalize timezone strings to standard offsets (e.g., `"+00:00"`) before exporting the FFI stream, removing the requirement for consumers to bundle heavy timezone databases just to parse `"UTC"`.

## Summary
The current Rust ADBC driver performs significant "last-mile" data conversion (scaled integers, timestamp reconstruction, schema adjustment). If `sf_core` were to export canonical Arrow types (`Decimal128`, `Timestamp(unit, tz)`) directly, the `ConvertingReader` in the ADBC driver could be completely eliminated, resulting in a zero-copy, highly performant passthrough.
