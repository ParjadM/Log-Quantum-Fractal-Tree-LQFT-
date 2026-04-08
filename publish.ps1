# LQFT Production Release (v1.2.2)
# Architect: Parjad Minooei
# Target: McMaster B.Tech Portfolio

$Version = "v1.2.2"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " INITIATING MCMASTER PORTFOLIO RELEASE: $Version" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. CLEAN BUILD ARTIFACTS ONLY
$Artifacts = @("build", "dist", "lqft_python_engine.egg-info", "__pycache__", "wheelhouse")
foreach ($folder in $Artifacts) {
    if (Test-Path $folder) { Remove-Item -Recurse -Force $folder }
}
Get-ChildItem -Filter "*.pyd" -Recurse | Remove-Item -Force
Get-ChildItem -Filter "*.so" -Recurse | Remove-Item -Force

# 2. WRITE VERSION MARKER
Write-Host "[*] Writing release version marker..." -ForegroundColor Cyan
$Version | Out-File -FilePath "version.txt" -Encoding utf8

# 3. RELEASE NOTE SUMMARY
Write-Host "[*] Release note summary:" -ForegroundColor Cyan
Write-Host "    - Keep the public API focused on LQFT and MutableLQFT." -ForegroundColor Gray
Write-Host "    - MutableLQFT improved materially in write-heavy, mixed, and churn-heavy local benchmarking." -ForegroundColor Gray
Write-Host "    - Persistent LQFT improved in native-backed reads and writes, but some results remain benchmark-dependent." -ForegroundColor Gray
Write-Host "    - This release should not claim broad superiority over dict/hash tables." -ForegroundColor Gray

# 4. GITHUB SYNC
Write-Host "[*] Staging $Version release source..." -ForegroundColor Cyan
git add .
git commit -m "release: $Version - public API cleanup and native mutable uplift"
git push origin main

# 5. TAG RELEASE
Write-Host "[*] Pushing tag $Version to trigger PyPI build..." -ForegroundColor Cyan
git tag $Version
git push origin $Version

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " DEPLOYMENT $Version LIVE" -ForegroundColor Green
Write-Host " Release notes are intentionally conservative about practical performance." -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green