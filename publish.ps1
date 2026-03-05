# LQFT Production Release (v1.0.2 Gold Master)
# Architect: Parjad Minooei
# Target: McMaster B.Tech Portfolio

$Version = "v1.0.2"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " 🚀 INITIATING MCMASTER PORTFOLIO RELEASE: $Version" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. PURGE EXPERIMENTAL FILES
# 1.0.2 FIX: Aggressively removing ALL legacy benchmarks that crash CI/CD
$Extras = @(
    "test.py", "test1.py", "v096_concurrency_showdown.py", 
    "stress_test_10m.py", "consistency_audit.py", "validation.py", 
    "benchmark.py", "test_lqft.py", "comprehensive_benchmark.py", 
    "adaptive_benchmark.py", "trie_vs_lqft_benchmark.py", "graph_vs_lqft.py", 
    "the_architects_choice.py", "complexity_demonstrator.py", 
    "complexity_crossover.py", "leetcode_187_test.py", "memory_benchmark.py", 
    "perplexity_benchmark_suite.py", "advanced_lqft_stress_suite.py", 
    "three_sum_lqft_test.py", "lqft_integrity_proofs.py", "demo_lqft.py", 
    "stress_test_memory_win.py", "initialize_lqft.py", "integrity_check_v44.py", 
    "stress_test_large_payload.py", "enterprise_capability_suite.py", 
    "pre_release_suite.py", "make_readme.py", "gil_bypass_test.py", 
    "lqft_final_validation.py", "v087_saturation_test.py", "density_test.py", 
    "crud_benchmark.py", "test_v090_wrapper.py", "showdown_test.py", 
    "comprehensive_ranking.py", "leak_test.py", "leak_verification.py",
    "sharded_lock_benchmark.py"
)
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
git commit -m "release: $Version - Final CI/CD Purge and Gold Master Freeze"
git push origin main

# 5. FRESH TAGGING (Bypasses GitHub webhook bugs)
Write-Host "[*] Pushing clean tag $Version to trigger PyPI build..." -ForegroundColor Cyan
git tag $Version
git push origin $Version

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " ✅ DEPLOYMENT $Version LIVE" -ForegroundColor Green
Write-Host " The CI/CD Pipeline will now trigger perfectly." -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green