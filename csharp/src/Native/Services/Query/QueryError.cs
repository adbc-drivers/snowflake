using System;

namespace AdbcDrivers.Snowflake.Native.Services.Query;

/// <summary>
/// Represents a query execution error.
/// </summary>
internal class QueryError
{
    /// <summary>
    /// Gets or sets the error code.
    /// </summary>
    public string ErrorCode { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the error message.
    /// </summary>
    public string Message { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the originating exception, when the failure came from one — carried so the
    /// statement layer can rethrow with the full stack/inner chain instead of a flattened message.
    /// </summary>
    public Exception? Exception { get; set; }
}