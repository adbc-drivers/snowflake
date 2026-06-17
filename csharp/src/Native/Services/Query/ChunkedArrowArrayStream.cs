using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Apache.Arrow;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using Ipc = Apache.Arrow.Ipc;

namespace AdbcDrivers.Snowflake.Native.Services.Query;

internal sealed class ChunkedArrowArrayStream : Ipc.IArrowArrayStream
{
    private readonly Channel<PrefetchedChunk> _channel;
    private readonly CancellationTokenSource _cts;
    private readonly Task _prefetchTask;
    private Ipc.ArrowStreamReader? _currentReader;
    private Stream? _currentStream;
    private bool _disposed;

    public Schema Schema { get; }

    private ChunkedArrowArrayStream(
        Schema schema,
        Ipc.ArrowStreamReader firstReader,
        Stream firstStream,
        Channel<PrefetchedChunk> channel,
        CancellationTokenSource cts,
        Task prefetchTask)
    {
        Schema = schema;
        _currentReader = firstReader;
        _currentStream = firstStream;
        _channel = channel;
        _cts = cts;
        _prefetchTask = prefetchTask;
    }

    public static async Task<ChunkedArrowArrayStream> CreateAsync(
        IRestApiClient apiClient,
        AuthenticationToken authToken,
        string? rowSetBase64,
        List<ChunkInfo>? chunks,
        Dictionary<string, string>? chunkHeaders,
        string? qrmk,
        CancellationToken cancellationToken,
        int prefetchConcurrency = 10)
    {
        var chunkList = (chunks ?? []).Where(c => !string.IsNullOrWhiteSpace(c.Url)).ToList();

        Stream firstStream;
        Ipc.ArrowStreamReader firstReader;

        if (!string.IsNullOrEmpty(rowSetBase64))
        {
            var arrowBytes = Convert.FromBase64String(rowSetBase64);
            firstStream = new MemoryStream(arrowBytes);
            firstReader = new Ipc.ArrowStreamReader(firstStream);
        }
        else if (chunkList.Count > 0)
        {
            var first = chunkList[0];
            chunkList.RemoveAt(0);
            firstStream = await apiClient.GetArrowStreamAsync(first.Url, authToken, chunkHeaders, qrmk, cancellationToken).ConfigureAwait(false);
            firstReader = new Ipc.ArrowStreamReader(firstStream);
        }
        else
        {
            throw new InvalidOperationException("Arrow result format was requested, but neither rowsetBase64 nor chunks were present.");
        }

        var schema = firstReader.Schema;
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var bufferSize = Math.Min(chunkList.Count, prefetchConcurrency);
        var channel = Channel.CreateBounded<PrefetchedChunk>(new BoundedChannelOptions(Math.Max(bufferSize, 1))
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        Task prefetchTask;
        if (chunkList.Count > 0)
        {
            prefetchTask = StartPrefetchAsync(apiClient, authToken, chunkHeaders, qrmk, chunkList, channel, cts, prefetchConcurrency);
        }
        else
        {
            // No external chunks (the whole result is inline in rowsetBase64). Complete the
            // channel now so that, once the inline batch is consumed, ReadNextRecordBatchAsync's
            // WaitToReadAsync returns false instead of blocking forever waiting for a writer.
            channel.Writer.TryComplete();
            prefetchTask = Task.CompletedTask;
        }

        return new ChunkedArrowArrayStream(schema, firstReader, firstStream, channel, cts, prefetchTask);
    }

    private static Task StartPrefetchAsync(
        IRestApiClient apiClient,
        AuthenticationToken authToken,
        Dictionary<string, string>? chunkHeaders,
        string? qrmk,
        List<ChunkInfo> chunks,
        Channel<PrefetchedChunk> channel,
        CancellationTokenSource cts,
        int maxConcurrency)
    {
        return Task.Run(async () =>
        {
            // Sliding window of launched-but-not-yet-handed-off downloads. A chunk keeps its
            // window slot from launch until the channel accepts it, so total resident chunks are
            // bounded at ~2x maxConcurrency (window + channel) no matter how slowly the consumer
            // reads — a slow consumer back-pressures the downloads instead of the whole result
            // set accumulating in memory. (Releasing slots at download *completion* would let a
            // slow consumer buffer every chunk of the result set.) The trade-off is head-of-line:
            // while the oldest download is still in flight, later completed chunks hold their
            // slots and no new downloads launch; with roughly uniform chunk sizes this keeps the
            // pipe ~full for a fast consumer.
            var window = new Queue<Task<PrefetchedChunk>>(maxConcurrency);
            int next = 0;

            try
            {
                while (next < chunks.Count || window.Count > 0)
                {
                    while (next < chunks.Count && window.Count < maxConcurrency)
                    {
                        cts.Token.ThrowIfCancellationRequested();
                        window.Enqueue(DownloadChunkAsync(apiClient, authToken, chunkHeaders, qrmk, chunks[next], cts.Token));
                        next++;
                    }

                    PrefetchedChunk chunk = await window.Dequeue().ConfigureAwait(false);
                    try
                    {
                        await channel.Writer.WriteAsync(chunk, cts.Token).ConfigureAwait(false);
                    }
                    catch
                    {
                        // The consumer never received it; it is ours to clean up.
                        await chunk.Stream.DisposeAsync().ConfigureAwait(false);
                        throw;
                    }
                }

                channel.Writer.TryComplete();
            }
            catch (OperationCanceledException)
            {
                cts.Cancel();
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                // Stop the sibling downloads promptly rather than leaving them running
                // orphaned, and surface the failure to the consumer.
                cts.Cancel();
                channel.Writer.TryComplete(ex);
            }
            finally
            {
                // Settle any downloads still in the window (siblings in flight after a failure /
                // cancellation), disposing the buffers they produced so they don't leak and their
                // exceptions don't go unobserved.
                while (window.Count > 0)
                {
                    try { await (await window.Dequeue().ConfigureAwait(false)).Stream.DisposeAsync().ConfigureAwait(false); }
                    catch { /* cancelled or failed download -- nothing to dispose */ }
                }
            }
        });
    }

    private static async Task<PrefetchedChunk> DownloadChunkAsync(
        IRestApiClient apiClient,
        AuthenticationToken authToken,
        Dictionary<string, string>? chunkHeaders,
        string? qrmk,
        ChunkInfo chunk,
        CancellationToken cancellationToken)
    {
        // GetArrowStreamAsync returns as soon as the HTTP headers arrive (and only
        // wraps the live network/gzip stream). Fully buffer the chunk here so the
        // expensive part -- the body transfer + decompression -- happens in parallel
        // across the prefetch workers, not serially on the consumer thread. The
        // consumer then just does CPU-bound Arrow decode from memory.
        await using var netStream = await apiClient.GetArrowStreamAsync(chunk.Url, authToken, chunkHeaders, qrmk, cancellationToken).ConfigureAwait(false);
        // Pre-size from the server-reported uncompressed size: these are multi-megabyte
        // (large-object-heap) buffers, so growth-doubling would copy each one several times.
        var buffer = new MemoryStream(Math.Max(chunk.UncompressedSize, 0));
        await netStream.CopyToAsync(buffer, cancellationToken).ConfigureAwait(false);
        buffer.Position = 0;
        return new PrefetchedChunk(buffer);
    }

    public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_currentReader != null)
            {
                var batch = await _currentReader.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);
                if (batch != null)
                    return batch;
                DisposeCurrentReaderAndStream();
            }

            if (!await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                return null;

            if (!_channel.Reader.TryRead(out var nextChunk))
                return null;

            _currentStream = nextChunk.Stream;
            _currentReader = new Ipc.ArrowStreamReader(_currentStream);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cts.Cancel();
        DisposeCurrentReaderAndStream();
        while (_channel.Reader.TryRead(out var chunk))
            chunk.Stream.Dispose();
        try { _prefetchTask.GetAwaiter().GetResult(); } catch { }
        _cts.Dispose();
    }

    private void DisposeCurrentReaderAndStream()
    {
        try { _currentReader?.Dispose(); } finally { _currentStream?.Dispose(); }
        _currentReader = null;
        _currentStream = null;
    }

    private readonly record struct PrefetchedChunk(Stream Stream);
}
