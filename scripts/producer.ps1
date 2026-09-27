<#
.SYNOPSIS
Producer run: scrape one publisher, land its files in the lake, and push
scraper health to the warehouse.

.DESCRIPTION
Steps:
  1. ingest run --publisher <p> [--limit N]
  2. Docker preflight: objectstore and warehouse must report healthy.
     Otherwise steps 3-4 are skipped; the scraped files stay on disk and the
     next run's `lake land` mirrors them.
  3. lake land --publisher <p>
  4. ops push-scrape-health --publisher <p> --run-log <ingest.err.log>
     --ingest-exit-code <code>

Land and push run even after a failed ingest: what was written is durable,
and the push records the run as incomplete.

Every Python step runs through Start-Process with its output redirected to
files, never through `2>&1`. Windows PowerShell 5.1 would otherwise turn
stderr lines into ErrorRecords and write UTF-16 files. PYTHONUTF8=1 makes
Python write UTF-8 to those files.

Logs: logs\producer\<UTC yyyyMMddTHHmmssZ>-<publisher>\ holds
ingest.out.log, ingest.err.log (structlog JSON), land.log, push.log and
producer.log. The last line of producer.log is the run summary.

Exit codes, highest priority first when several steps fail:
  5  another producer run holds logs\producer\.lock
  3  Docker services unavailable; land and push skipped
  1  ingest failed (or a fault in this script, logged in producer.log)
  2  land failed
  4  push failed
  0  success

PL_PRODUCER_DRY_RUN=1 skips the ingest step with exit code 0 and still runs
the preflight, land and push. It exists only to verify these mechanics
without scraping.

.PARAMETER Publisher
Publisher to run. Default: sciencedirect.

.PARAMETER Limit
Truncate the enabled journal list to this many rows. 0 (the default) means
no limit.
#>
[CmdletBinding()]
param(
    [string]$Publisher = 'sciencedirect',
    [ValidateRange(0, 2147483647)]
    [int]$Limit = 0
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
$producerDir = Join-Path $root 'logs\producer'
$lockPath = Join-Path $producerDir '.lock'
$utf8 = New-Object System.Text.UTF8Encoding($false)
$invariant = [System.Globalization.CultureInfo]::InvariantCulture
$dryRun = $env:PL_PRODUCER_DRY_RUN -eq '1'
$env:PYTHONUTF8 = '1'
Set-Location -LiteralPath $root

$startedAt = [DateTime]::UtcNow
$runDir = Join-Path $producerDir ('{0}-{1}' -f $startedAt.ToString("yyyyMMdd'T'HHmmss'Z'", $invariant), $Publisher)
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$producerLog = Join-Path $runDir 'producer.log'

function Write-ProducerLine([string]$Line) {
    [System.IO.File]::AppendAllText($producerLog, $Line + "`r`n", $utf8)
    [Console]::Out.WriteLine($Line)
}

function Write-ProducerLog([string]$Message) {
    Write-ProducerLine ('{0} {1}' -f [DateTime]::UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", $invariant), $Message)
}

function ConvertTo-ArgumentString([string[]]$Arguments) {
    # Start-Process joins an argument array without quoting, so quote each
    # argument by the MSVCRT rules that python.exe and docker.exe parse.
    $quoted = foreach ($argument in $Arguments) {
        if ($argument -eq '') {
            '""'
        } elseif ($argument -notmatch '[\s"]') {
            $argument
        } else {
            '"' + (($argument -replace '(\\*)"', '$1$1\"') -replace '(\\+)$', '$1$1') + '"'
        }
    }
    return ($quoted -join ' ')
}

function Invoke-Logged([string]$FilePath, [string[]]$Arguments, [string]$StdOut, [string]$StdErr) {
    $process = Start-Process -FilePath $FilePath -ArgumentList (ConvertTo-ArgumentString $Arguments) `
        -WorkingDirectory $root -NoNewWindow -Wait -PassThru `
        -RedirectStandardOutput $StdOut -RedirectStandardError $StdErr
    return [int]$process.ExitCode
}

function Add-FileBytes([string]$Target, [string]$Source) {
    # Byte-level append keeps both files' UTF-8 exactly as written.
    if (-not (Test-Path -LiteralPath $Source)) { return }
    $bytes = [System.IO.File]::ReadAllBytes($Source)
    $stream = [System.IO.File]::Open($Target, [System.IO.FileMode]::Append)
    try { $stream.Write($bytes, 0, $bytes.Length) } finally { $stream.Dispose() }
    Remove-Item -LiteralPath $Source
}

function Invoke-PythonStep([string]$Name, [string[]]$Arguments, [string]$LogFile) {
    # stdout goes to the step log; stderr is appended to it afterwards,
    # because Start-Process cannot redirect both streams to one file.
    $errFile = "$LogFile.stderr"
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $code = Invoke-Logged $python (@('-m', 'publisher_lakehouse.cli') + $Arguments) $LogFile $errFile
    } catch {
        Write-ProducerLog "$Name could not start: $($_.Exception.Message)"
        return 1
    }
    Add-FileBytes $LogFile $errFile
    Write-ProducerLog ('{0} exit={1} ({2:N0}s)' -f $Name, $code, $watch.Elapsed.TotalSeconds)
    return $code
}

function Get-JsonProperty($Object, [string]$Name) {
    $property = $Object.PSObject.Properties[$Name]
    if ($property) { return $property.Value }
    return $null
}

function Test-DockerServices {
    # Returns an empty string when objectstore and warehouse are healthy,
    # otherwise the reason they are not.
    $outFile = Join-Path $runDir 'docker-ps.out.tmp'
    $errFile = Join-Path $runDir 'docker-ps.err.tmp'
    try {
        try {
            $code = Invoke-Logged 'docker' @('compose', 'ps', '--format', 'json') $outFile $errFile
        } catch {
            return "docker could not start: $($_.Exception.Message)"
        }
        if ($code -ne 0) {
            $detail = ([System.IO.File]::ReadAllText($errFile, $utf8).Trim() -split "`r?`n")[0]
            return "docker compose ps failed (exit $code): $detail"
        }
        $text = [System.IO.File]::ReadAllText($outFile, $utf8).Trim()
        # Compose prints one JSON object per line; older releases print an array.
        $documents = if ($text.StartsWith('[')) { @($text) } else { @($text -split "`r?`n" | Where-Object { $_.Trim() }) }
        try {
            $services = foreach ($document in $documents) { foreach ($item in ($document | ConvertFrom-Json)) { $item } }
        } catch {
            return "docker compose ps output is not JSON: $($_.Exception.Message)"
        }
        $problems = foreach ($name in @('objectstore', 'warehouse')) {
            $service = $services | Where-Object { (Get-JsonProperty $_ 'Service') -eq $name } | Select-Object -First 1
            if ($null -eq $service) {
                "$name is not running"
            } elseif ((Get-JsonProperty $service 'Health') -ne 'healthy') {
                '{0} is {1}/{2}' -f $name, (Get-JsonProperty $service 'State'), (Get-JsonProperty $service 'Health')
            }
        }
        return (@($problems) -join '; ')
    } finally {
        Remove-Item -LiteralPath $outFile, $errFile -ErrorAction SilentlyContinue
    }
}

function Get-LockHolder {
    # Returns the live PID that holds the lock, or 0 for a stale lock.
    try {
        $info = Get-Item -LiteralPath $lockPath
        $lines = [System.IO.File]::ReadAllLines($lockPath, $utf8)
    } catch {
        return 0
    }
    $values = @{}
    foreach ($line in $lines) {
        $key, $value = $line -split '=', 2
        if ($null -ne $value) { $values[$key.Trim()] = $value.Trim() }
    }
    $holder = 0
    if (-not ($values.ContainsKey('pid') -and [int]::TryParse($values['pid'], [ref]$holder))) {
        # A lock that is still being written is not stale yet.
        if (([DateTime]::UtcNow - $info.LastWriteTimeUtc).TotalSeconds -lt 60) { return -1 }
        return 0
    }
    $process = Get-Process -Id $holder -ErrorAction SilentlyContinue
    if ($null -eq $process) { return 0 }
    $lockedAt = [DateTime]::MinValue
    if ($values.ContainsKey('started') -and [DateTime]::TryParse(
            $values['started'], $invariant, [System.Globalization.DateTimeStyles]::RoundtripKind, [ref]$lockedAt)) {
        try {
            # A reused PID belongs to a process that started after the lock.
            if ($process.StartTime.ToUniversalTime() -gt $lockedAt.ToUniversalTime()) { return 0 }
        } catch {
            # StartTime is unreadable for another user's process: assume live.
        }
    }
    return $holder
}

function Enter-ProducerLock {
    # Returns 0 when this run holds the lock, otherwise the holder's PID
    # (-1 when the holder is unknown).
    New-Item -ItemType Directory -Force -Path $producerDir | Out-Null
    for ($attempt = 1; $attempt -le 2; $attempt++) {
        $stream = $null
        try {
            $stream = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::Read)
        } catch {
            if (-not (Test-Path -LiteralPath $lockPath)) { throw }
            $holder = Get-LockHolder
            if ($holder -ne 0) { return $holder }
            $previous = ([System.IO.File]::ReadAllText($lockPath, $utf8).Trim() -split "`r?`n") -join ' '
            Write-ProducerLog "replacing stale lock ($previous)"
            Remove-Item -LiteralPath $lockPath -Force
            continue
        }
        try {
            $bytes = $utf8.GetBytes(('pid={0}{1}started={2}{1}' -f $PID, "`r`n", $startedAt.ToString('o', $invariant)))
            $stream.Write($bytes, 0, $bytes.Length)
        } finally {
            $stream.Dispose()
        }
        return 0
    }
    return -1
}

function Exit-ProducerLock {
    # Remove the lock only while it is still this run's.
    try {
        $line = [System.IO.File]::ReadAllLines($lockPath, $utf8) | Where-Object { $_ -like 'pid=*' } | Select-Object -First 1
        if ($line -eq "pid=$PID") { Remove-Item -LiteralPath $lockPath -Force }
    } catch {
        Write-ProducerLog "could not release lock: $($_.Exception.Message)"
    }
}

$ingest = 'skipped'
$land = 'skipped'
$marker = 'skipped'
$push = 'skipped'
$exitCode = 0
$locked = $false

Write-ProducerLog ('producer start: publisher={0} limit={1} dry_run={2} logs={3}' -f $Publisher, $Limit, $dryRun, $runDir)
$holder = Enter-ProducerLock
if ($holder -ne 0) {
    $shown = if ($holder -gt 0) { $holder } else { 'unknown' }
    Write-ProducerLog "another producer run is active (pid $shown)"
    $exitCode = 5
} else {
    $locked = $true
}

try {
    if ($locked) {
        $ingestOut = Join-Path $runDir 'ingest.out.log'
        $ingestErr = Join-Path $runDir 'ingest.err.log'
        if ($dryRun) {
            [System.IO.File]::WriteAllText($ingestOut, "PL_PRODUCER_DRY_RUN=1: ingest step skipped`r`n", $utf8)
            [System.IO.File]::WriteAllText($ingestErr, '', $utf8)
            $ingest = 0
            Write-ProducerLog 'ingest skipped (PL_PRODUCER_DRY_RUN=1), exit=0'
        } else {
            $ingestArgs = @('-m', 'publisher_lakehouse.cli', 'ingest', 'run', '--publisher', $Publisher)
            if ($Limit -gt 0) { $ingestArgs += @('--limit', [string]$Limit) }
            $watch = [System.Diagnostics.Stopwatch]::StartNew()
            try {
                $ingest = Invoke-Logged $python $ingestArgs $ingestOut $ingestErr
            } catch {
                Write-ProducerLog "ingest could not start: $($_.Exception.Message)"
                $ingest = 1
            }
            Write-ProducerLog ('ingest exit={0} ({1:N0}s)' -f $ingest, $watch.Elapsed.TotalSeconds)
        }

        $dockerProblem = Test-DockerServices
        if ($dockerProblem) {
            Write-ProducerLog "Docker preflight failed: $dockerProblem. Skipping land and push; the scraped files stay on disk and the next run's lake land mirrors them."
            $exitCode = 3
        } else {
            Write-ProducerLog 'Docker preflight ok: objectstore and warehouse healthy'
            $landLog = Join-Path $runDir 'land.log'
            $landCode = Invoke-PythonStep 'land' @('lake', 'land', '--publisher', $Publisher) $landLog
            $land = if ($landCode -eq 0) { 'ok' } else { 'failed' }
            $markerLine = $null
            if (Test-Path -LiteralPath $landLog) {
                $markerLine = [System.IO.File]::ReadAllLines($landLog, $utf8) | Where-Object { $_ -like 'marker: *' } | Select-Object -First 1
            }
            $marker = if ($markerLine) { 'written' } else { 'none' }
            if ($markerLine) { Write-ProducerLog $markerLine }

            $pushArgs = @(
                'ops', 'push-scrape-health', '--publisher', $Publisher,
                '--run-log', $ingestErr, '--ingest-exit-code', [string]$ingest
            )
            $pushCode = Invoke-PythonStep 'push' $pushArgs (Join-Path $runDir 'push.log')
            $push = if ($pushCode -eq 0) { 'ok' } else { 'failed' }

            if ($ingest -ne 0) { $exitCode = 1 }
            elseif ($land -eq 'failed') { $exitCode = 2 }
            elseif ($push -eq 'failed') { $exitCode = 4 }
        }
    }
} catch {
    # Not a step failure: a fault in this script itself.
    Write-ProducerLog "producer script error: $($_.Exception.Message)"
    if ($exitCode -eq 0) { $exitCode = 1 }
} finally {
    if ($locked) { Exit-ProducerLock }
    Write-ProducerLine ('Producer {0}: ingest={1} land={2} marker={3} push={4} exit={5}' -f $Publisher, $ingest, $land, $marker, $push, $exitCode)
}
exit $exitCode
