$ErrorActionPreference = "Stop"

$HostUrl = if ($env:HOST) { $env:HOST } else { "http://127.0.0.1:8080" }
$Username = if ($env:USERNAME) { $env:USERNAME } else { "perf_user_" + [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() }
$Password = if ($env:PASSWORD) { $env:PASSWORD } else { "pass123456" }
$Title = if ($env:TITLE) { $env:TITLE } else { "perf video" }
$Description = if ($env:DESC) { $env:DESC } else { "perf generated video" }
$PlayUrl = if ($env:PLAY_URL) { $env:PLAY_URL } else { "http://example.com/perf.mp4" }
$CoverUrl = if ($env:COVER_URL) { $env:COVER_URL } else { "http://example.com/perf.jpg" }

$registerBody = @{
  username = $Username
  password = $Password
} | ConvertTo-Json -Compress

try {
  Invoke-RestMethod -Uri "$HostUrl/account/register" -Method Post -ContentType "application/json" -Body $registerBody | Out-Null
} catch {
}

$loginResp = Invoke-RestMethod -Uri "$HostUrl/account/login" -Method Post -ContentType "application/json" -Body $registerBody
if (-not $loginResp.token) {
  throw "failed to login and extract token"
}

$publishBody = @{
  title = $Title
  description = $Description
  play_url = $PlayUrl
  cover_url = $CoverUrl
} | ConvertTo-Json -Compress

$headers = @{ Authorization = "Bearer $($loginResp.token)" }
$publishResp = Invoke-RestMethod -Uri "$HostUrl/video/publish" -Method Post -ContentType "application/json" -Headers $headers -Body $publishBody
if (-not $publishResp.id) {
  throw "failed to publish video and extract video id"
}

Write-Output "HOST=$HostUrl"
Write-Output "USERNAME=$Username"
Write-Output "TOKEN=$($loginResp.token)"
Write-Output "VIDEO_ID=$($publishResp.id)"
