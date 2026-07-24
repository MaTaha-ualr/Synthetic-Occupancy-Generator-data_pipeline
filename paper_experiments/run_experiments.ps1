$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$BaselineCsv = "phase1/outputs_phase1/Phase1_people_addresses.csv"
$BaselineManifest = "phase1/outputs_phase1/Phase1_people_addresses.manifest.json"

if (-not (Test-Path $BaselineCsv)) {
    throw "Missing Phase-1 baseline: $BaselineCsv"
}
if (-not (Test-Path $BaselineManifest)) {
    throw "Missing Phase-1 manifest: $BaselineManifest"
}

$ResultsDir = "paper_experiments/results"
New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null
$LogPath = Join-Path $ResultsDir "generation.log"
Remove-Item $LogPath -ErrorAction SilentlyContinue

$Scenarios = @(
    "paper_clean",
    "paper_high_noise",
    "paper_low_overlap",
    "paper_one_to_many"
)

foreach ($Scenario in $Scenarios) {
    Write-Host "`nRunning $Scenario..." -ForegroundColor Cyan
    python phase2/scripts/run_phase2_pipeline.py `
        --scenario-yaml "paper_experiments/scenarios/$Scenario.yaml" `
        --run-date 2026-07-20 `
        --overwrite `
        --rebuild-population `
        --no-progress 2>&1 | Tee-Object -FilePath $LogPath -Append

    if ($LASTEXITCODE -ne 0) {
        throw "Scenario failed: $Scenario"
    }
}

Write-Host "`nEvaluating the fixed baseline matcher..." -ForegroundColor Cyan
python paper_experiments/evaluate_baseline.py
if ($LASTEXITCODE -ne 0) {
    throw "Baseline evaluation failed."
}

Write-Host "`nCompleted. Results are in paper_experiments/results/" -ForegroundColor Green
