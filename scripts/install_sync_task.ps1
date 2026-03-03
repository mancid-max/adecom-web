param(
  [string]$TaskName = "ADECOM_Data_AutoSync",
  [string]$SourceDir = "C:\Users\manuh\Desktop\APIS\Documentos a cargar ADECOM WEB",
  [int]$IntervalSeconds = 30
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$scriptPath = Join-Path $repoRoot "scripts\sync_data_to_git.ps1"

if (!(Test-Path $scriptPath)) {
  throw "No existe script: $scriptPath"
}

$ps = "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe"
$args = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" -Watch -SourceDir `"$SourceDir`" -RepoDir `"$repoRoot`" -IntervalSeconds $IntervalSeconds"

schtasks /Create /TN $TaskName /TR "`"$ps`" $args" /SC ONLOGON /RL LIMITED /F | Out-Host
if ($LASTEXITCODE -ne 0) {
  throw "No se pudo crear la tarea programada."
}

Write-Host "[task] Tarea creada: $TaskName"
Write-Host "[task] Ejecutable: $ps"
Write-Host "[task] Script: $scriptPath"
