# Stage G5: Automated Backup & Rotation Script for NYC Taxi Big Data Platform
# Implements the 3-tier backup strategy from the Mid-Term Database Report:
#   1. PostgreSQL analytical mart (`analytics` schema: dims, etl_load_log, model_metrics, predictions)
#   2. Gold / Silver / Bronze JSON lineage manifests & ML contracts
#   3. Frozen ML model artifacts (`models/gold-v3` and `distribution/models`)

param(
    [string]$BackupRoot = "$PSScriptRoot\..\backups",
    [int]$KeepCount = 5,
    [string]$PostgresContainer = "nyc-taxi-postgres",
    [string]$PostgresUser = "nyc_taxi",
    [string]$PostgresDb = "nyc_taxi"
)

$ErrorActionPreference = "Stop"
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$TargetDir = Join-Path $BackupRoot "snapshot-$Timestamp"
New-Item -ItemType Directory -Force -Path $TargetDir | Out-Null

Write-Host "[1/4] Backing up PostgreSQL schema & ML/ETL tables from container '$PostgresContainer'..."
$PgDumpFile = Join-Path $TargetDir "nyc_taxi_analytics_metadata_and_ml.sql"
docker exec $PostgresContainer pg_dump -U $PostgresUser -d $PostgresDb `
    --schema=analytics `
    --exclude-table-data=analytics.fact_trip `
    > $PgDumpFile
if ($LASTEXITCODE -ne 0) {
    throw "pg_dump failed with exit code $LASTEXITCODE"
}

Write-Host "[2/4] Backing up Medallion Pipeline manifests and ML contracts..."
$ManifestBackupDir = Join-Path $TargetDir "manifests_and_contracts"
New-Item -ItemType Directory -Force -Path $ManifestBackupDir | Out-Null
$ProjectRoot = Resolve-Path "$PSScriptRoot\.."
$FilesToCopy = @(
    "data\raw\raw_manifest.json",
    "data\bronze\bronze_manifest.json",
    "data\silver\silver_manifest.json",
    "data\gold\gold_manifest.json",
    "data\ml\gold-v3\preparation_report.json",
    "config\gold_ml_v3_contract.json",
    "config\pretrip_v2_contract.json",
    "config\pretrip_v2_calendar.json"
)
foreach ($RelPath in $FilesToCopy) {
    $Src = Join-Path $ProjectRoot $RelPath
    if (Test-Path $Src) {
        Copy-Item -Path $Src -Destination (Join-Path $ManifestBackupDir (Split-Path $RelPath -Leaf)) -Force
    }
}

Write-Host "[3/4] Backing up Gold V3 trained model artifacts..."
$ModelSrc = Join-Path $ProjectRoot "models\gold-v3"
if (Test-Path $ModelSrc) {
    Copy-Item -Path $ModelSrc -Destination (Join-Path $TargetDir "gold-v3-model") -Recurse -Force
}

Write-Host "[4/4] Enforcing retention policy (keeping latest $KeepCount snapshots)..."
$Snapshots = Get-ChildItem -Path $BackupRoot -Directory -Filter "snapshot-*" | Sort-Object Name -Descending
if ($Snapshots.Count -gt $KeepCount) {
    $Snapshots | Select-Object -Skip $KeepCount | ForEach-Object {
        Write-Host "  Removing old snapshot: $($_.FullName)"
        Remove-Item -Path $_.FullName -Recurse -Force
    }
}

Write-Host "Backup completed successfully at: $TargetDir"
