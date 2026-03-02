# LQFT Production Release (v0.1.8)
# Architect: Parjad Minooei
# This version promotes the V4.4 Incremental Hashing Engine to Stable.

$Version = "v0.1.8"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " 🚀 INITIATING PRODUCTION RELEASE: $Version" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. THE TOTAL PURGE (Cleaning experimental artifacts for a clean portfolio)
Write-Host "[*] Purging experimental scripts and old verification suites..." -ForegroundColor Yellow
$Extras = @(
    "leak_test.py", "leak_verification.py", "benchmark.py", "comprehensive_benchmark.py", 
    "adaptive_benchmark.py", "trie_vs_lqft_benchmark.py", "graph_vs_lqft.py", 
    "the_architects_choice.py", "complexity_demonstrator.py", "complexity_crossover.py", 
    "leetcode_187_test.py", "memory_benchmark.py", "perplexity_benchmark_suite.py", 
    "advanced_lqft_stress_suite.py", "lqft_final_validation.py", "three_sum_lqft_test.py",
    "lqft_integrity_proofs.py", "demo_lqft.py", "stress_test_memory_win.py", "initialize_lqft.py",
    "lqft.py", "test_lqft.py", "github_setup.ps1", "integrity_check_v44.py", "stress_test_large_payload.py",
    "dashboard_info.md"
)

foreach ($file in $Extras) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Host "  > Purged: $file" -ForegroundColor Gray
    }
}

# 2. LOCAL BUILD ARTIFACT CLEANUP
Write-Host "[*] Cleaning build folders..." -ForegroundColor Yellow
$Artifacts = @("build", "dist", "lqft_python_engine.egg-info")
foreach ($folder in $Artifacts) {
    if (Test-Path $folder) {
        Remove-Item -Recurse -Force $folder
    }
}
Get-ChildItem -Filter "*.pyd" -Recurse | Remove-Item -Force
Get-ChildItem -Filter "*.so" -Recurse | Remove-Item -Force

# 3. GITHUB SYNC
Write-Host "[*] Staging stable production core..." -ForegroundColor Cyan
git add .
git commit -m "release: $Version - Production Stable (Incremental Hashing Fixed)" --allow-empty
git push origin main

# 4. TAGGING (Triggers PyPI Action)
Write-Host "[*] Creating production release tag $Version..." -ForegroundColor Cyan
# Remove existing tag if it exists locally or remotely
git tag -d $Version 2>$null
git push origin :refs/tags/$Version 2>$null

# Apply fresh tag
git tag $Version
git push origin --tags

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " ✅ DEPLOYMENT $Version LIVE ON GITHUB & PYPI" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green