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

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.Query;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Offline tests for <see cref="ChunkedArrowArrayStream"/>'s prefetch scheduling, using a fake
/// <see cref="IRestApiClient"/> that serves in-memory Arrow chunks. Focus: the download window
/// must bound memory when the consumer is slow, without losing chunks or ordering.
/// </summary>
[Trait("Category", "Unit")]
public class ChunkedArrowArrayStreamTests
{
    private static readonly AuthenticationToken Token = new() { SessionToken = "session" };

    /// <summary>Serializes a single-column Arrow stream whose one batch holds the given value.</summary>
    private static byte[] ArrowChunkBytes(int value)
    {
        var schema = new Schema([new Field("v", Int32Type.Default, false)], null);
        using var batch = new RecordBatch(schema, [new Int32Array.Builder().Append(value).Build()], 1);
        using var buffer = new MemoryStream();
        using (var writer = new ArrowStreamWriter(buffer, schema))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }
        return buffer.ToArray();
    }

    private static List<ChunkInfo> Chunks(int count)
    {
        var chunks = new List<ChunkInfo>(count);
        for (int i = 0; i < count; i++)
            chunks.Add(new ChunkInfo { Url = $"https://chunks.test/{i}", UncompressedSize = 128 });
        return chunks;
    }

    /// <summary>
    /// Fake client: serves chunk i as an Arrow stream containing the value i, counting the
    /// download calls it has received.
    /// </summary>
    private sealed class FakeChunkClient : IRestApiClient
    {
        private int _downloads;

        public int Downloads => Volatile.Read(ref _downloads);

        public Task<ApiResponse<TResponse>> PostAsync<TRequest, TResponse>(
            string endpoint, TRequest request, AuthenticationToken token, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<ApiResponse<TResponse>> GetAsync<TResponse>(
            string endpoint, AuthenticationToken token, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<Stream> GetArrowStreamAsync(
            string url, AuthenticationToken token, Dictionary<string, string>? chunkHeaders = null,
            string? qrmk = null, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _downloads);
            int index = int.Parse(url[(url.LastIndexOf('/') + 1)..]);
            return Task.FromResult<Stream>(new MemoryStream(ArrowChunkBytes(index)));
        }
    }

    [Fact]
    public async Task SlowConsumer_BoundsResidentChunks_ToWindowPlusChannel()
    {
        // Given 30 chunks with prefetch concurrency 4 and a consumer that reads NOTHING yet.
        // Downloads complete instantly, so without handoff-gated slots the prefetcher would
        // race through all 30; with the window it must stall once window (4) + channel (4)
        // + the one blocked in WriteAsync are occupied.
        const int chunkCount = 30;
        const int concurrency = 4;
        var client = new FakeChunkClient();

        using var stream = await ChunkedArrowArrayStream.CreateAsync(
            client, Token,
            rowSetBase64: Convert.ToBase64String(ArrowChunkBytes(-1)), // inline first batch; all 30 go to the prefetcher
            Chunks(chunkCount), chunkHeaders: null, qrmk: null,
            CancellationToken.None, concurrency);

        // When the prefetcher is given time to run as far as it can without any reads
        await Task.Delay(500);
        int downloadsWhileStalled = client.Downloads;

        // Then it launched at most window + channel + 1 (the write in flight), far below all 30
        Assert.InRange(downloadsWhileStalled, concurrency, 2 * concurrency + 1);

        // And when the consumer finally drains the stream, every chunk arrives exactly once, in order
        var values = new List<int>();
        while (await stream.ReadNextRecordBatchAsync() is { } batch)
        {
            using (batch)
                values.Add(((Int32Array)batch.Column(0)).GetValue(0)!.Value);
        }

        Assert.Equal(chunkCount + 1, values.Count);
        Assert.Equal(-1, values[0]); // the inline batch
        for (int i = 0; i < chunkCount; i++)
            Assert.Equal(i, values[i + 1]);
        Assert.Equal(chunkCount, client.Downloads);
    }

    [Fact]
    public async Task FastConsumer_ReceivesEveryChunkInOrder()
    {
        // Given 25 chunks and a consumer that drains as fast as batches arrive
        const int chunkCount = 25;
        var client = new FakeChunkClient();

        using var stream = await ChunkedArrowArrayStream.CreateAsync(
            client, Token,
            rowSetBase64: Convert.ToBase64String(ArrowChunkBytes(-1)),
            Chunks(chunkCount), chunkHeaders: null, qrmk: null,
            CancellationToken.None, prefetchConcurrency: 6);

        var values = new List<int>();
        while (await stream.ReadNextRecordBatchAsync() is { } batch)
        {
            using (batch)
                values.Add(((Int32Array)batch.Column(0)).GetValue(0)!.Value);
        }

        // Then all chunks arrive exactly once, in order, each downloaded exactly once
        Assert.Equal(chunkCount + 1, values.Count);
        for (int i = 0; i < chunkCount; i++)
            Assert.Equal(i, values[i + 1]);
        Assert.Equal(chunkCount, client.Downloads);
    }

    [Fact]
    public async Task Dispose_WithUnconsumedChunks_ReturnsPromptly()
    {
        // Given a stream whose prefetcher is stalled on a slow consumer (nothing read)
        var client = new FakeChunkClient();
        var stream = await ChunkedArrowArrayStream.CreateAsync(
            client, Token,
            rowSetBase64: Convert.ToBase64String(ArrowChunkBytes(-1)),
            Chunks(20), chunkHeaders: null, qrmk: null,
            CancellationToken.None, prefetchConcurrency: 4);
        await Task.Delay(200); // let the window fill and the writer block

        // When the stream is disposed mid-flight, it unwinds the prefetcher promptly (no hang)
        var dispose = Task.Run(stream.Dispose);
        Assert.True(await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(5))) == dispose,
            "Dispose did not complete within 5s — prefetch task is stuck.");
    }
}
