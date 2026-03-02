# LQFT Release Automation & Repository Purge
# Architect: Parjad Minooei

$Version = "v0.1.6"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " 🚀 INITIATING RELEASE: $Version" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. DELETING EXTRA FILES (Cleaning the Workspace)
Write-Host "[*] Purging experimental/extra files..." -ForegroundColor Yellow
$Extras = @(
    "demo_lqft.py", 
    "stress_test_memory_win.py", 
    "three_sum_lqft_test.py", 
    "lqft_integrity_proofs.py", 
    "complexity_demonstrator.py", 
    "trie_vs_lqft_benchmark.py", 
    "graph_vs_lqft.py", 
    "the_architects_choice.py", 
    "comprehensive_benchmark.py", 
    "benchmark.py", 
    "adaptive_benchmark.py", 
    "leetcode_187_test.py", 
    "complexity_crossover.py", 
    "initialize_lqft.py",
    "github_setup.ps1",
    "leak_test.py",
    "leak_verification.py"
)

foreach ($file in $Extras) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Host "  > Deleted: $file" -ForegroundColor Gray
    }
}

# 2. LOCAL BUILD CLEANUP
Write-Host "[*] Cleaning local build artifacts..." -ForegroundColor Yellow
$Artifacts = @("build", "dist", "lqft_python_engine.egg-info")
foreach ($folder in $Artifacts) {
    if (Test-Path $folder) {
        Remove-Item -Recurse -Force $folder
    }
}
Get-ChildItem -Filter "*.pyd" -Recurse | Remove-Item -Force
Get-ChildItem -Filter "*.so" -Recurse | Remove-Item -Force

# 3. GITHUB SYNC
Write-Host "[*] Staging core engine and release manifest..." -ForegroundColor Cyan
git add .
git commit -m "release: $Version - full memory-safe engine and distribution manifest"
git push origin main

# 4. PYPI TRIGGER (Tagging)
Write-Host "[*] Tagging release $Version to trigger GitHub Actions..." -ForegroundColor Cyan
git tag $Version
git push origin --tags

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " ✅ DEPLOYMENT TRIGGERED SUCCESSFULLY" -ForegroundColor Green
Write-Host " Monitoring: https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-/actions" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green