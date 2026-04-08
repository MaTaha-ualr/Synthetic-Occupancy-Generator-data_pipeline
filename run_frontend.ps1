$Host.UI.RawUI.WindowTitle = "SOG Benchmark Studio"

Write-Host ""
Write-Host "SOG Benchmark Studio" -ForegroundColor Cyan
Write-Host "Launching Streamlit frontend..." -ForegroundColor Gray
Write-Host ""

$env:PYTHONUNBUFFERED = "1"

try {
    $pythonVersion = python --version 2>&1
    Write-Host "Detected $pythonVersion" -ForegroundColor DarkGray
} catch {
    Write-Host "Python was not found. Install Python 3.10+ first." -ForegroundColor Red
    exit 1
}

python -u -m streamlit run frontend/chatbot_production.py `
    --server.headless true `
    --server.port 8501 `
    --browser.gatherUsageStats false

Write-Host ""
Write-Host "Streamlit server stopped." -ForegroundColor Gray
