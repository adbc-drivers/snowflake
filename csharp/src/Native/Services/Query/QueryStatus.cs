namespace AdbcDrivers.Snowflake.Native.Services.Query;

/// <summary>
/// Represents query execution status.
/// </summary>
internal enum QueryStatus
{
    /// <summary>
    /// Query completed successfully.
    /// </summary>
    Success,

    /// <summary>
    /// Query failed with an error.
    /// </summary>
    Failed,

    /// <summary>
    /// Query was cancelled.
    /// </summary>
    Cancelled
}