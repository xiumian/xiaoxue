param([ValidateSet('start','stop','restart','status')][string]$Action='status')
$ErrorActionPreference='Stop'
$root=Split-Path $PSScriptRoot -Parent
$exe=Join-Path $root '.venv\Scripts\python.exe'
$entry=Join-Path $root 'bot.py'
$pidFile=Join-Path $root 'runtime\bot.pid'

function Get-OwnedBot {
    if(-not (Test-Path -LiteralPath $pidFile)){ return $null }
    $text=(Get-Content -LiteralPath $pidFile -Raw).Trim()
    if($text -notmatch '^\d+$'){ throw 'Invalid bot PID file' }
    $proc=Get-CimInstance Win32_Process -Filter "ProcessId=$text"
    if(-not $proc){return $null}
    if($proc.ExecutablePath -ne $exe -or $proc.CommandLine -notmatch 'bot\.py'){
        throw 'PID belongs to a different process; refusing to manage it'
    }
    return $proc
}

function Get-OwnedChildren($proc) {
    @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$($proc.ProcessId)" | Where-Object {
        $_.Name -eq 'python.exe' -and $_.CommandLine -match 'bot\.py'
    })
}

function Get-OwnedListener($proc) {
    if(-not $proc){return $null}
    $ids=@([int]$proc.ProcessId)+@(Get-OwnedChildren $proc | ForEach-Object {[int]$_.ProcessId})
    Get-NetTCPConnection -State Listen -LocalPort 6199 -ErrorAction SilentlyContinue | Where-Object { $_.OwningProcess -in $ids }
}

if($Action -in @('stop','restart')){
    $proc=Get-OwnedBot
    if($proc){
        # Windows venv python.exe launches the actual interpreter as a child.
        foreach($child in (Get-OwnedChildren $proc)){
            Stop-Process -Id $child.ProcessId -ErrorAction SilentlyContinue
            Wait-Process -Id $child.ProcessId -Timeout 10 -ErrorAction SilentlyContinue
        }
        if(Get-Process -Id $proc.ProcessId -ErrorAction SilentlyContinue){
            Stop-Process -Id $proc.ProcessId -ErrorAction SilentlyContinue
            Wait-Process -Id $proc.ProcessId -Timeout 10 -ErrorAction SilentlyContinue
        }
        Write-Output 'Bot stopped; NapCat unchanged.'
    }
}
if($Action -in @('start','restart')){
    $proc=Get-OwnedBot
    if($proc){Write-Output 'Bot already running.'; return}
    $listeners=Get-NetTCPConnection -State Listen -LocalPort 6199 -ErrorAction SilentlyContinue
    if($listeners){throw 'Port 6199 is already occupied; no process was stopped'}
    $logs=Join-Path $root 'runtime\logs'
    New-Item -ItemType Directory -Force -Path $logs | Out-Null
    $proc=Start-Process -FilePath $exe -ArgumentList @('-u',('"'+$entry+'"')) -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logs 'bot-stdout.log') -RedirectStandardError (Join-Path $logs 'bot-stderr.log') -PassThru
    $proc.Id | Set-Content -LiteralPath $pidFile -Encoding utf8
    $listener=$null
    for($attempt=0;$attempt -lt 10;$attempt++){
        Start-Sleep -Seconds 1
        $owned=Get-OwnedBot
        if(-not $owned){throw 'Bot exited; inspect runtime/logs/bot-stderr.log'}
        $listener=Get-OwnedListener $owned
        if($listener){break}
    }
    if(-not $listener){throw 'Bot did not open its local port'}
    Write-Output 'Bot started; localhost:6199; NapCat unchanged.'
}
if($Action -eq 'status'){
    $proc=Get-OwnedBot
    [pscustomobject]@{Running=[bool]$proc;PID=if($proc){$proc.ProcessId}else{$null};Listener=[bool](Get-OwnedListener $proc)} | ConvertTo-Json -Compress
}
