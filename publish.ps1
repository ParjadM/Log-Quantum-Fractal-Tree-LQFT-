# LQFT Production Release (v0.9.4)
# Architect: Parjad Minooei
# Status: Billion-Scale Stability Release - Hardened Production Core

$Version = "v0.9.4"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " 🚀 INITIATING PRODUCTION RELEASE: $Version" -ForegroundColor Magenta
Write-Host " Status: Billion-Snapshot Memory Folding & 13M Search locked" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. THE TOTAL PURGE (Keeping the MScAC Portfolio focused)
Write-Host "[*] Purging experimental stress suites and ranking benchmarks..." -ForegroundColor Yellow
$Extras = @(
    "test.py", "stress_test_10m.py", "comprehensive_ranking.py", 
    "arena_saturation_test.py", "crud_stability_test.py", "elite_benchmark.py", 
    "grand_finale_benchmark.py", "test1.py", "leak_test.py", "leak_verification.py", 
    "comprehensive_benchmark.py", "adaptive_benchmark.py", "trie_vs_lqft_benchmark.py", 
    "graph_vs_lqft.py", "the_architects_choice.py", "complexity_demonstrator.py", 
    "complexity_crossover.py", "leetcode_187_test.py", "memory_benchmark.py", 
    "perplexity_benchmark_suite.py", "advanced_lqft_stress_suite.py", 
    "three_sum_lqft_test.py", "lqft_integrity_proofs.py", "demo_lqft.py", 
    "stress_test_memory_win.py", "initialize_lqft.py", "github_setup.ps1", 
    "integrity_check_v44.py", "stress_test_large_payload.py",
    "enterprise_capability_suite.py", "pre_release_suite.py", "make_readme.py",
    "gil_bypass_test.py", "lqft_final_validation.py", "v087_saturation_test.py",
    "density_test.py", "crud_benchmark.py", "test_v090_wrapper.py", "showdown_test.py"
)

foreach ($file in $Extras) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Host "  > Purged: $file" -ForegroundColor Gray
    }
}

# 2. LOCAL BUILD ARTIFACT CLEANUP
Write-Host "[*] Cleaning local build environments..." -ForegroundColor Yellow
$Artifacts = @("build", "dist", "lqft_python_engine.egg-info", "__pycache__")
foreach ($folder in $Artifacts) {
    if (Test-Path $folder) {
        Remove-Item -Recurse -Force $folder
    }
}
Get-ChildItem -Filter "*.pyd" -Recurse | Remove-Item -Force
Get-ChildItem -Filter "*.so" -Recurse | Remove-Item -Force

# 3. GITHUB SYNC
Write-Host "[*] Staging v0.9.4 production source..." -ForegroundColor Cyan
git add .
git commit -m "release: $Version - Production Hardened Core with Billion-Scale Snapshot Folding" --allow-empty
git push origin main

# 4. TAGGING (Triggers GitHub Actions PyPI Build)
Write-Host "[*] Updating production tag to $Version..." -ForegroundColor Cyan
git tag -d $Version 2>$null
git push origin :refs/tags/$Version 2>$null

git tag $Version
git push origin --tags

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " ✅ DEPLOYMENT $Version LIVE" -ForegroundColor Green
Write-Host " The Billion-Scale Persistence Engine is officially sealed." -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green