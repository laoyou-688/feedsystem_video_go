$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$PerfDir = Join-Path $RootDir "scripts\perf"

$HostUrl = if ($env:HOST) { $env:HOST } else { "http://127.0.0.1:8080" }
$VideoId = $env:VIDEO_ID
$Connections = if ($env:CONNECTIONS) { [int]$env:CONNECTIONS } else { 50 }
$Threads = if ($env:THREADS) { [int]$env:THREADS } else { 4 }
$Duration = if ($env:DURATION) { $env:DURATION } else { "15s" }
$WrkBin = if ($env:WRK_BIN) { $env:WRK_BIN } else { "wrk.exe" }

if (-not $VideoId) {
  throw "VIDEO_ID is required. Run scripts/perf/prepare_perf_data.ps1 first."
}

$wrkCmd = Get-Command $WrkBin -ErrorAction SilentlyContinue
if (-not $wrkCmd) {
  throw "missing wrk binary: $WrkBin"
}

$luaTemplate = Get-Content -Raw (Join-Path $PerfDir "video_detail.lua")
$tmpLua = Join-Path $env:TEMP ("video_detail_" + [guid]::NewGuid().ToString("N") + ".lua")
$luaTemplate.Replace("__VIDEO_ID__", $VideoId) | Set-Content -Path $tmpLua -NoNewline

try {
  Write-Output "== prewarm auto path =="
  $env:CACHE_MODE = "auto"
  & $wrkCmd.Source -t1 -c1 -d2s -s $tmpLua "$HostUrl/video/getDetail" | Out-Null

  foreach ($mode in @("local", "redis", "mysql")) {
    Write-Output ""
    Write-Output "== $mode cache path =="
    $env:CACHE_MODE = $mode
    & $wrkCmd.Source "-t$Threads" "-c$Connections" "-d$Duration" -s $tmpLua "$HostUrl/video/getDetail"
  }
} finally {
  Remove-Item -LiteralPath $tmpLua -ErrorAction SilentlyContinue
  Remove-Item Env:CACHE_MODE -ErrorAction SilentlyContinue
}
