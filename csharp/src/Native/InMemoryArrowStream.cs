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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

using Apache.Arrow;

namespace AdbcDrivers.Snowflake.Native;

/// <summary>
/// A lightweight <see cref="IArrowArrayStream"/> that yields a single, pre-built
/// record batch held in memory. Used for metadata results (e.g. GetTableTypes,
/// GetInfo, GetObjects) where the entire result is constructed up front.
/// </summary>
internal sealed class InMemoryArrowStream(Schema schema, IReadOnlyList<IArrowArray> data) : IArrowArrayStream
{
    private RecordBatch? _batch = new(schema, data, data[0].Length);

    public Schema Schema => schema;

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        RecordBatch? batch = _batch;
        _batch = null;
        return new ValueTask<RecordBatch?>(batch);
    }

    public void Dispose()
    {
        _batch?.Dispose();
        _batch = null;
    }
}
