# LQFT Final Production Release & Workspace Purge
# Architect: Parjad Minooei
# Version: v0.1.7 (Zero-Footprint C-Engine Build)

$Version = "v0.1.7"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " 🚀 INITIATING FINAL PRODUCTION PURGE & RELEASE: $Version" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. THE TOTAL PURGE (Cleaning the workspace of all experimental artifacts)
# This ensures your MScAC portfolio looks like a professional production repo.
Write-Host "[*] Purging all experimental benchmarks and technical guides..." -ForegroundColor Yellow
$Extras = @(
    # Benchmark & Verification Scripts
    "leak_test.py", "leak_verification.py", "benchmark.py", "comprehensive_benchmark.py", 
    "adaptive_benchmark.py", "trie_vs_lqft_benchmark.py", "graph_vs_lqft.py", 
    "the_architects_choice.py", "complexity_demonstrator.py", "complexity_crossover.py", 
    "leetcode_187_test.py", "memory_benchmark.py", "perplexity_benchmark_suite.py", 
    "advanced_lqft_stress_suite.py", "lqft_final_validation.py", "three_sum_lqft_test.py",
    "lqft_integrity_proofs.py", "demo_lqft.py", "stress_test_memory_win.py", "initialize_lqft.py",
    "lqft.py", "test_lqft.py",

    # Documentation & Guides
    "lqft_blog_post.md", "announcements.md", "roadmap.md", "performance_report.md", 
    "lqft_discovery.md", "vs_setup_guide.md", "vscode_setup_guide.md", "troubleshooting_guide.md", 
    "lqft_complexity_analysis.md", "complexity_analysis_summary.md", "lqft_algorithm_logic.md", 
    "lqft_strategic_roadmap.md", "lqft_vs_all.md",
    
    # Utility Scripts
    "github_setup.ps1"
)

foreach ($file in $Extras) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Host "  > Purged: $file" -ForegroundColor Gray
    }
}

# 2. LOCAL BUILD ARTIFACT CLEANUP
Write-Host "[*] Cleaning local build folders..." -ForegroundColor Yellow
$Artifacts = @("build", "dist", "lqft_python_engine.egg-info", "lqft_python.egg-info")
foreach ($folder in $Artifacts) {
    if (Test-Path $folder) {
        Remove-Item -Recurse -Force $folder
    }
}
Get-ChildItem -Filter "*.pyd" -Recurse | Remove-Item -Force
Get-ChildItem -Filter "*.so" -Recurse | Remove-Item -Force

# 3. GITHUB SYNC
Write-Host "[*] Staging production core and CI manifests..." -ForegroundColor Cyan
git add .
git commit -m "release: $Version - Finalized Zero-Footprint C-Engine (Dynamic Registry)" --allow-empty
git push origin main

# 4. TAGGING (This is the critical trigger for the PyPI Release Action)
Write-Host "[*] Updating release tag $Version..." -ForegroundColor Cyan
# Safely remove existing tag locally and remotely to ensure v0.1.7 is fresh
git tag -d $Version 2>$null
git push origin :refs/tags/$Version 2>$null

# Create and push the new production tag
git tag $Version
git push origin --tags

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " ✅ REPOSITORY IS CLEAN | DEPLOYMENT v0.1.7 LIVE" -ForegroundColor Green
Write-Host " GitHub: https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green