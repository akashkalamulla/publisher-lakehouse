<#
.SYNOPSIS
Define the daily Windows Task Scheduler task that runs scripts\producer.ps1.

.DESCRIPTION
The task runs as the current user, only while that user is logged on
(Interactive logon, "Run only when user is logged on"). The scraper drives a
real browser and needs the user's desktop session, so the task never uses
"Run whether user is logged on or not" and stores no password.

Settings: a second start while a run is active is ignored, a run is stopped
after 10 hours, a missed start runs as soon as possible, and battery power
neither prevents nor stops a run.

.PARAMETER At
Daily start time, HH:mm in local time. Default: 02:00.

.PARAMETER PrintOnly
Print the task XML without registering anything.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File scripts\register_producer_task.ps1 -At 02:00
#>
[CmdletBinding()]
param(
    [ValidatePattern('^([01][0-9]|2[0-3]):[0-5][0-9]$')]
    [string]$At = '02:00',
    [switch]$PrintOnly
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

$taskName = 'PublisherLakehouse-Producer'
$root = Split-Path -Parent $PSScriptRoot
$producer = Join-Path $root 'scripts\producer.ps1'
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$start = [DateTime]::ParseExact($At, 'HH:mm', [System.Globalization.CultureInfo]::InvariantCulture)

$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument ('-NoProfile -ExecutionPolicy Bypass -File "{0}" -Publisher sciencedirect' -f $producer) `
    -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Daily -At $start
# The cmdlet writes a UTC boundary, which pins the trigger to UTC. Without an
# offset the task starts at this local wall-clock time.
$trigger.StartBoundary = $start.ToString("yyyy-MM-dd'T'HH:mm:ss", [System.Globalization.CultureInfo]::InvariantCulture)
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 10) -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings `
    -Description 'Publisher Lakehouse producer: ingest run, lake land, and scrape-health push (scripts\producer.ps1).'

if ($PrintOnly) {
    Export-ScheduledTask -InputObject $task
    return
}

Register-ScheduledTask -TaskName $taskName -InputObject $task -Force | Out-Null
Write-Output ('Registered {0}: daily at {1} as {2}, only while logged on.' -f $taskName, $At, $user)
Write-Output ('Logs: {0}' -f (Join-Path $root 'logs\producer'))
Write-Output 'To unregister:'
Write-Output ("  Unregister-ScheduledTask -TaskName '{0}' -Confirm:`$false" -f $taskName)
