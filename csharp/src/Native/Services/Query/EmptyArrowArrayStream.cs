/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Ipc = Apache.Arrow.Ipc;

namespace AdbcDrivers.Snowflake.Native.Services.Query;

/// <summary>
/// An <see cref="Ipc.IArrowArrayStream"/> that carries a schema but yields no record batches.
/// Used for successful queries whose result set is empty: Snowflake sends neither
/// <c>rowsetBase64</c> nor chunks for a zero-row Arrow result, so the schema is built from the
/// response's <c>rowtype</c> metadata instead.
/// </summary>
internal sealed class EmptyArrowArrayStream(Schema schema) : Ipc.IArrowArrayStream
{
    public Schema Schema { get; } = schema;

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
        new((RecordBatch?)null);

    public void Dispose()
    {
    }
}
