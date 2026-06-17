using Apache.Arrow;

namespace AdbcDrivers.Snowflake.Native.Services.Query;

/// <summary>
/// Represents a prepared statement. Snowflake's protocol reports only the result columns from a
/// describe — never bind-parameter types — so there is deliberately no parameter schema here
/// (<c>GetParameterSchema</c> throws NotImplemented for the same reason).
/// </summary>
internal class PreparedStatement
{
    /// <summary>
    /// Gets or sets the result schema (if known).
    /// </summary>
    public Schema? ResultSchema { get; set; }
}