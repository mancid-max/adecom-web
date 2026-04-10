param(
  [string]$SourceDir = "C:\Users\manuh\Desktop\APIS\Documentos a cargar ADECOM WEB",
  [string]$RepoDir = "",
  [switch]$Watch,
  [int]$IntervalSeconds = 30
)

$ErrorActionPreference = "Stop"

function Resolve-RepoDir {
  param([string]$InputDir)
  if ($InputDir -and $InputDir.Trim()) {
    return (Resolve-Path $InputDir).Path
  }
  return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Find-SourceFile {
  param(
    [string]$Dir,
    [string]$Token,
    [string]$ExcludeToken = ""
  )
  if (!(Test-Path $Dir)) { return $null }
  $tokenN = ($Token -replace "[^a-zA-Z0-9]", "").ToLowerInvariant()
  $excludeN = ($ExcludeToken -replace "[^a-zA-Z0-9]", "").ToLowerInvariant()
  $files = Get-ChildItem -Path $Dir -File
  $foundFiles = @()
  foreach ($f in $files) {
    $nameN = ($f.Name -replace "[^a-zA-Z0-9]", "").ToLowerInvariant()
    if ($nameN -notmatch $tokenN) { continue }
    if ($excludeN -and $nameN -match $excludeN) { continue }
    $foundFiles += $f
  }
  if ($foundFiles.Count -eq 0) { return $null }
  return ($foundFiles | Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1)
}

function Find-SourceFiles {
  param(
    [string]$Dir,
    [string]$Token,
    [string]$ExcludeToken = ""
  )
  if (!(Test-Path $Dir)) { return @() }
  $tokenN = ($Token -replace "[^a-zA-Z0-9]", "").ToLowerInvariant()
  $excludeN = ($ExcludeToken -replace "[^a-zA-Z0-9]", "").ToLowerInvariant()
  $files = Get-ChildItem -Path $Dir -File
  $foundFiles = @()
  foreach ($f in $files) {
    $nameN = ($f.Name -replace "[^a-zA-Z0-9]", "").ToLowerInvariant()
    if ($nameN -notmatch $tokenN) { continue }
    if ($excludeN -and $nameN -match $excludeN) { continue }
    $foundFiles += $f
  }
  if ($foundFiles.Count -eq 0) { return @() }
  return ($foundFiles | Sort-Object Name)
}

function Stage-And-Push {
  param([string]$RepoRoot)

  $seedRel = @(
    "seed/SALDOS-SECCI*.TXT",
    "seed/SALDOS-SECCI*.txt",
    "seed/PEDIDOSXTALLA.TXT",
    "seed/Grande-Adecom.TXT"
  )

  & git -C $RepoRoot add -- $seedRel
  if ($LASTEXITCODE -ne 0) {
    throw "git add fallo."
  }

  $staged = & git -C $RepoRoot diff --cached --name-only
  if ([string]::IsNullOrWhiteSpace(($staged -join ""))) {
    Write-Host "[sync] Sin cambios para commit."
    return
  }

  $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  $msg = "Auto-sync data files ($stamp)"
  & git -C $RepoRoot commit -m $msg | Out-Host
  if ($LASTEXITCODE -ne 0) {
    throw "git commit fallo."
  }

  & git -C $RepoRoot push origin main | Out-Host
  if ($LASTEXITCODE -ne 0) {
    throw "git push fallo."
  }

  Write-Host "[sync] Push completado."
}

function Sync-Now {
  param(
    [string]$FromDir,
    [string]$RepoRoot
  )

  $seedDir = Join-Path $RepoRoot "seed"
  New-Item -ItemType Directory -Path $seedDir -Force | Out-Null

  $saldosFiles = Find-SourceFiles -Dir $FromDir -Token "SALDOS-SECCI" -ExcludeToken ""
  if ($saldosFiles.Count -eq 0) {
    throw "No se encontraron archivos: SALDOS-SECCI"
  }
  Get-ChildItem -Path $seedDir -File -Filter "SALDOS-SECCI*.TXT" -ErrorAction SilentlyContinue | Remove-Item -Force
  Get-ChildItem -Path $seedDir -File -Filter "SALDOS-SECCI*.txt" -ErrorAction SilentlyContinue | Remove-Item -Force
  foreach ($src in $saldosFiles) {
    $dst = Join-Path $seedDir $src.Name
    Copy-Item -Path $src.FullName -Destination $dst -Force
    Write-Host ("[sync] Copiado: {0} -> {1}" -f $src.Name, $src.Name)
  }

  $map = @(
    @{ token = "PEDIDOSXTALLA"; exclude = "TODAS"; target = "PEDIDOSXTALLA.TXT" },
    @{ token = "Grande-Adecom"; exclude = ""; target = "Grande-Adecom.TXT" }
  )

  $missing = @()
  foreach ($item in $map) {
    $src = Find-SourceFile -Dir $FromDir -Token $item.token -ExcludeToken $item.exclude
    if ($null -eq $src) {
      $missing += $item.token
      continue
    }
    $dst = Join-Path $seedDir $item.target
    Copy-Item -Path $src.FullName -Destination $dst -Force
    Write-Host ("[sync] Copiado: {0} -> {1}" -f $src.Name, $item.target)
  }

  if ($missing.Count -gt 0) {
    throw ("No se encontraron archivos: " + ($missing -join ", "))
  }

  Stage-And-Push -RepoRoot $RepoRoot
}

function Snapshot-Signatures {
  param([string]$Dir)
  $sign = @{}
  if (!(Test-Path $Dir)) { return $sign }
  foreach ($f in Get-ChildItem -Path $Dir -File) {
    $key = $f.FullName
    $value = "{0}|{1}" -f $f.Length, $f.LastWriteTimeUtc.Ticks
    $sign[$key] = $value
  }
  return $sign
}

function Has-Changes {
  param(
    [hashtable]$Prev,
    [hashtable]$Curr
  )
  if ($Prev.Count -ne $Curr.Count) { return $true }
  foreach ($k in $Curr.Keys) {
    if (!$Prev.ContainsKey($k)) { return $true }
    if ($Prev[$k] -ne $Curr[$k]) { return $true }
  }
  return $false
}

$repoRoot = Resolve-RepoDir -InputDir $RepoDir
if (!(Test-Path $SourceDir)) {
  throw "No existe la carpeta origen: $SourceDir"
}
if (!(Test-Path (Join-Path $repoRoot ".git"))) {
  throw "No se detecto .git en el repo: $repoRoot"
}

if (-not $Watch) {
  Sync-Now -FromDir $SourceDir -RepoRoot $repoRoot
  exit 0
}

Write-Host "[sync] Modo watch activo."
Write-Host "[sync] Origen: $SourceDir"
Write-Host "[sync] Repo:   $repoRoot"

$prev = Snapshot-Signatures -Dir $SourceDir
while ($true) {
  Start-Sleep -Seconds $IntervalSeconds
  $curr = Snapshot-Signatures -Dir $SourceDir
  if (-not (Has-Changes -Prev $prev -Curr $curr)) {
    continue
  }
  $prev = $curr
  try {
    Sync-Now -FromDir $SourceDir -RepoRoot $repoRoot
  } catch {
    Write-Host ("[sync] Error: {0}" -f $_.Exception.Message)
  }
}
