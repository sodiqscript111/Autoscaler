param(
    [string]$Url = "http://localhost:8080/events",
    [int]$DurationSeconds = 30,
    [int]$Concurrency = 100,
    [int]$RequestsPerSecond = 0
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Net.Http

$client = [System.Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromSeconds(5)

$ok = 0
$throttled = 0
$failed = 0
$sent = 0
$eventID = 0
$tasks = [System.Collections.Generic.List[object]]::new()
$start = Get-Date
$nextLog = $start.AddSeconds(1)
$delayMs = 0

if ($RequestsPerSecond -gt 0) {
    $delayMs = [Math]::Max([int](1000 / $RequestsPerSecond), 1)
}

function New-EventJson {
    param([int]$ID)

    $event = [ordered]@{
        id        = $ID
        user_id   = "load-test-user-$($ID % 20)"
        action    = "click"
        element   = "load-test-button-$($ID % 5)"
        duration  = [Math]::Round((Get-Random -Minimum 10 -Maximum 900) / 1000, 3)
        timestamp = (Get-Date).ToUniversalTime().ToString("o")
    }

    return ($event | ConvertTo-Json -Compress)
}

function Start-Request {
    param(
        [System.Net.Http.HttpClient]$Client,
        [string]$TargetUrl,
        [string]$Json
    )

    $content = [System.Net.Http.StringContent]::new($Json, [System.Text.Encoding]::UTF8, "application/json")
    return $Client.PostAsync($TargetUrl, $content)
}

function Drain-Completed {
    param([System.Collections.Generic.List[object]]$TaskList)

    for ($i = $TaskList.Count - 1; $i -ge 0; $i--) {
        $task = $TaskList[$i]
        if (-not $task.IsCompleted) {
            continue
        }

        try {
            $status = [int]$task.Result.StatusCode
            if ($status -ge 200 -and $status -lt 300) {
                $script:ok++
            } elseif ($status -eq 429) {
                $script:throttled++
            } else {
                $script:failed++
            }
        } catch {
            $script:failed++
        }

        $TaskList.RemoveAt($i)
    }
}

$rateLabel = if ($RequestsPerSecond -gt 0) { "$RequestsPerSecond" } else { "uncapped" }

Write-Host "Starting load test: url=$Url duration=${DurationSeconds}s concurrency=$Concurrency rps=$rateLabel"
Write-Host "Watch the app terminal for [brain] and [manager] logs."

try {
    while (((Get-Date) - $start).TotalSeconds -lt $DurationSeconds) {
        Drain-Completed -TaskList $tasks

        while ($tasks.Count -ge $Concurrency) {
            [System.Threading.Tasks.Task]::WaitAny($tasks.ToArray(), 100) | Out-Null
            Drain-Completed -TaskList $tasks
        }

        $eventID++
        $json = New-EventJson -ID $eventID
        $tasks.Add((Start-Request -Client $client -TargetUrl $Url -Json $json))
        $sent++

        $now = Get-Date
        if ($now -ge $nextLog) {
            Write-Host ("sent={0} ok={1} throttled={2} failed={3} in_flight={4}" -f $sent, $ok, $throttled, $failed, $tasks.Count)
            $nextLog = $now.AddSeconds(1)
        }

        if ($delayMs -gt 0) {
            Start-Sleep -Milliseconds $delayMs
        }
    }

    while ($tasks.Count -gt 0) {
        [System.Threading.Tasks.Task]::WaitAny($tasks.ToArray(), 100) | Out-Null
        Drain-Completed -TaskList $tasks
    }
} finally {
    $client.Dispose()
}

Write-Host ("Done: sent={0} ok={1} throttled={2} failed={3}" -f $sent, $ok, $throttled, $failed)
