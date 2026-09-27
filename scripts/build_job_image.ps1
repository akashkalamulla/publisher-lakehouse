<#
.SYNOPSIS
Build the immutable Spark job image, labelled with the commit it contains.

.DESCRIPTION
GIT_COMMIT is `git rev-parse HEAD`, with `-dirty` appended when
`git status --porcelain` reports any change. The script runs
`docker compose --profile jobs build spark-job` with that value and prints
the image's org.opencontainers.image.revision label. Rebuild after every
commit that changes publisher_lakehouse/, so scheduled jobs run the
committed code.
#>
[CmdletBinding()]
param()

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$image = 'publisher-lakehouse-job:local'
Set-Location -LiteralPath $root

$sha = & git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw 'git rev-parse HEAD failed' }
$changes = & git status --porcelain
if ($LASTEXITCODE -ne 0) { throw 'git status --porcelain failed' }
$revision = if ($changes) { "$($sha.Trim())-dirty" } else { $sha.Trim() }

Write-Output "Building $image with GIT_COMMIT=$revision"
$previous = $env:GIT_COMMIT
$env:GIT_COMMIT = $revision
try {
    & docker compose --profile jobs build spark-job
    if ($LASTEXITCODE -ne 0) { throw "docker compose build failed with exit code $LASTEXITCODE" }
} finally {
    if ($null -eq $previous) { Remove-Item Env:GIT_COMMIT -ErrorAction SilentlyContinue } else { $env:GIT_COMMIT = $previous }
}

# Parse the inspect JSON: Windows PowerShell 5.1 strips the inner quotes a
# --format template would need.
$inspect = (& docker image inspect $image) -join "`n"
if ($LASTEXITCODE -ne 0) { throw "docker image inspect $image failed" }
# 5.1 emits a JSON array as one object and 7 enumerates it; foreach handles both.
$details = $null
foreach ($item in ($inspect | ConvertFrom-Json)) { $details = $item; break }
$label = $details.Config.Labels.PSObject.Properties['org.opencontainers.image.revision']
if ($null -eq $label -or $label.Value -ne $revision) {
    throw "$image revision label is '$(if ($label) { $label.Value })', expected '$revision'"
}
Write-Output "org.opencontainers.image.revision=$($label.Value)"
