# LQFT Production Release (v0.9.8)
# Architect: Parjad Minooei
# Target: McMaster B.Tech Portfolio

$Version = "v0.9.8"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " 🚀 INITIATING MCMASTER PORTFOLIO RELEASE: $Version" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. PURGE EXPERIMENTAL FILES
$Extras = @("test.py", "test1.py", "v096_concurrency_showdown.py", "stress_test_10m.py", "consistency_audit.py")
foreach ($file in $Extras) {
    if (Test-Path $file) { Remove-Item $file -Force }
}

# 2. CLEAN CACHE
$Artifacts = @("build", "dist", "lqft_python_engine.egg-info", "__pycache__", "wheelhouse")
foreach ($folder in $Artifacts) {
    if (Test-Path $folder) { Remove-Item -Recurse -Force $folder }
}
Get-ChildItem -Filter "*.pyd" -Recurse | Remove-Item -Force
Get-ChildItem -Filter "*.so" -Recurse | Remove-Item -Force

# 3. FORCE FILE CHANGE TO TRIGGER ACTIONS
Write-Host "[*] Forcing physical file change to trigger GitHub Actions..." -ForegroundColor Cyan
$Version | Out-File -FilePath "version.txt" -Encoding utf8

# 4. GITHUB SYNC
Write-Host "[*] Staging $Version production source..." -ForegroundColor Cyan
git add .
git commit -m "release: $Version - CI/CD Pipeline Hotfix (cibuildwheel & graceful FFI)"
git push origin main

# 5. FRESH TAGGING (Bypasses GitHub webhook bugs)
Write-Host "[*] Pushing clean tag $Version to trigger PyPI build..." -ForegroundColor Cyan
git tag $Version
git push origin $Version

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " ✅ DEPLOYMENT $Version LIVE" -ForegroundColor Green
Write-Host " The CI/CD Pipeline will now trigger perfectly." -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green