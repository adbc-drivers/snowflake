# Copyright (c) 2025 ADBC Drivers Contributors

# This file has been modified from its original version, which is
# under the Apache License:
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Write-Host "Building the Snowflake ADBC Go driver"
Write-Host "IsPackagingPipeline=$Env:IsPackagingPipeline"
Write-Host "IncludeGoSymbols=$Env:IncludeGoSymbols"

if (-not (Test-Path env:IsPackagingPipeline)) {
    Write-Host "IsPackagingPipeline environment variable does not exist."
    exit
}

# Get the value of the IsPackagingPipeline environment variable
$IsPackagingPipelineValue = $env:IsPackagingPipeline

# Get the value of the IncludeGoSymbols environment variable, default to false if not set
if (-not (Test-Path env:IncludeGoSymbols)) {
    Write-Host "IncludeGoSymbols environment variable does not exist. Defaulting to 'false'."
    $IncludeGoSymbolsValue = "false"
} else {
    $IncludeGoSymbolsValue = $env:IncludeGoSymbols
}

# Check if the value is "true"
if ($IsPackagingPipelineValue -ne "true") {
    Write-Host "IsPackagingPipeline is not set to 'true'. Exiting the script."
    exit
}

$location = Get-Location

$file = "libadbc_driver_snowflake.dll"

if(Test-Path $file)
{
    exit
}

cd ..\..\..\go\

if ($IncludeGoSymbolsValue -ne "true") {
    Write-Host "Building without symbols"
    pixi run make
} else {
    Write-Host "Building with symbols"
    go build -buildmode=c-shared -tags=driverlib -o build\$file -ldflags "-s=0 -w=0 -X github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion=unknown-dirty" -gcflags="all=-N -l" ./pkg
}

cd build

if(Test-Path $file)
{
    try {
        Write-Host "Copying file with robocopy (5 retries, 2 second wait)..."
        robocopy . $location $file /R:5 /W:2 /NP

        if ($LASTEXITCODE -le 7) {
            Write-Host "File copied successfully. Exit code: $LASTEXITCODE"
        } else {
            Write-Host "Failed to copy file. Robocopy exit code: $LASTEXITCODE"
            exit 1
        }
    }
    catch {
        Write-Host "Caught error: $_"
    }
 }
