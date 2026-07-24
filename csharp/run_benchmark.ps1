# Benchmark runner: native C# driver vs Go/Interop driver. Both projects expose an
# identically-shaped BenchmarkTests.BaselineQueryPerformance; this runs each suite 5
# times and parses the per-limit timings (the [NATIVE]/[INTEROP] log lines).

$ErrorActionPreference = "Stop"

# Determine the repository root from the location of this script.
# Assumes this script lives in the repository root. If you move it into
# a subdirectory (e.g. scripts\), change this to:
#   $repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$repoRoot = $PSScriptRoot

# Resolve project paths
$nativeProject = Join-Path $repoRoot "test\Native\AdbcDrivers.Snowflake.Native.Tests.csproj"
$interopProject = Join-Path $repoRoot "test\Interop\AdbcDrivers.Snowflake.Interop.Tests.csproj"

# Use an existing SNOWFLAKE_TEST_CONFIG_FILE environment variable if present.
# Otherwise prompt the user for the config file.
if (-not $env:SNOWFLAKE_TEST_CONFIG_FILE) {
    $env:SNOWFLAKE_TEST_CONFIG_FILE = Read-Host "Enter the full path to snowflakeconfig.local.json"
}

if (-not (Test-Path $env:SNOWFLAKE_TEST_CONFIG_FILE)) {
    throw "Snowflake config file not found: '$($env:SNOWFLAKE_TEST_CONFIG_FILE)'"
}

Write-Host "Using Snowflake config: $env:SNOWFLAKE_TEST_CONFIG_FILE"

$outDir = Join-Path $env:TEMP "adbc_bench"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
Get-ChildItem $outDir -Filter *.txt -ErrorAction SilentlyContinue | Remove-Item -Force

$runs = 5

Write-Output "########## NATIVE (C#) driver — BenchmarkTests ##########"
for ($i = 1; $i -le $runs; $i++) {
    Write-Output "---- native run $i/$runs ----"
    dotnet test $nativeProject -c Release --no-build `
        --filter "FullyQualifiedName~BenchmarkTests.BaselineQueryPerformance" `
        --logger "console;verbosity=detailed" 2>&1 |
        Tee-Object -FilePath "$outDir\native_$i.txt" | Out-Null
}

Write-Output "########## INTEROP (Go) driver — BenchmarkTests ##########"
for ($i = 1; $i -le $runs; $i++) {
    Write-Output "---- interop run $i/$runs ----"
    dotnet test $interopProject -c Release --no-build `
        --filter "FullyQualifiedName~BenchmarkTests.BaselineQueryPerformance" `
        --logger "console;verbosity=detailed" 2>&1 |
        Tee-Object -FilePath "$outDir\interop_$i.txt" | Out-Null
}

# ---- parse ----
function Get-Timings($glob) {
    $map = @{}
    foreach ($f in Get-ChildItem $outDir -Filter $glob) {
        foreach ($line in Get-Content $f.FullName) {
            if ($line -match 'for limit (\d+) in (\d+) ms') {
                $limit = [int]$Matches[1]
                $ms = [int]$Matches[2]
                if (-not $map.ContainsKey($limit)) {
                    $map[$limit] = @()
                }
                $map[$limit] += $ms
            }
        }
    }
    return $map
}

function Show-Stats($name, $map) {
    Write-Output ""
    Write-Output "===== $name ====="
    foreach ($limit in ($map.Keys | Sort-Object)) {
        $vals = $map[$limit]
        $mean = [math]::Round(($vals | Measure-Object -Average).Average, 0)
        $min  = ($vals | Measure-Object -Minimum).Minimum
        $max  = ($vals | Measure-Object -Maximum).Maximum

        if ($vals.Count -gt 1) {
            $sd = [math]::Round(
                [math]::Sqrt(
                    (($vals |
                        ForEach-Object { [math]::Pow($_ - $mean, 2) } |
                        Measure-Object -Sum).Sum) / ($vals.Count - 1)
                ),
                0
            )
        }
        else {
            $sd = 0
        }

        $joined = ($vals -join ', ')
        Write-Output ("limit {0,8}: n={1} mean={2,7} ms  min={3,7}  max={4,7}  sd={5,6}   [{6}]" -f $limit, $vals.Count, $mean, $min, $max, $sd, $joined)
    }
}

Write-Output ""
Write-Output "==================== RESULTS ===================="

$nat = Get-Timings "native_*.txt"
$intr = Get-Timings "interop_*.txt"

Show-Stats "NATIVE (C#)" $nat
Show-Stats "INTEROP (Go)" $intr

Write-Output ""
Write-Output "===== mean comparison (native / interop) ====="

foreach ($limit in ($nat.Keys | Sort-Object)) {
    if ($intr.ContainsKey($limit)) {
        $nm = ($nat[$limit] | Measure-Object -Average).Average
        $im = ($intr[$limit] | Measure-Object -Average).Average
        $ratio = [math]::Round($nm / $im, 2)

        Write-Output ("limit {0,8}: native {1,7} ms  vs  interop {2,7} ms   ratio={3}x" -f $limit, [math]::Round($nm, 0), [math]::Round($im, 0), $ratio)
    }
}

Write-Output "DONE"