/*
* Copyright (c) 2026 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*         http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Snowflake;

/// <summary>
/// Async forms of operations that never do I/O (local validation or in-memory results). The ADBC
/// base classes' defaults would run these on the thread pool; running them inline is cheaper, and
/// returning failures as faulted tasks keeps the async contract (callers see them on await).
/// </summary>
internal static class CompletedTasks
{
    public static Task<T> Run<T>(Func<T> operation, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled<T>(cancellationToken);

        try
        {
            return Task.FromResult(operation());
        }
        catch (Exception ex)
        {
            return Task.FromException<T>(ex);
        }
    }

    public static Task Run(Action operation, CancellationToken cancellationToken) =>
        Run(() => { operation(); return true; }, cancellationToken);
}
