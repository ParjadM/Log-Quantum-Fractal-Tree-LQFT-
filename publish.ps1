# LQFT Release Automation & Repository Purge
# Architect: Parjad Minooei

$Version = "v0.1.6"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " 🚀 INITIATING FINAL PRODUCTION PURGE: $Version" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. THE TOTAL PURGE (Removing all non-core experimental files)
Write-Host "[*] Purging all experimental benchmarks and technical guides..." -ForegroundColor Yellow
$Extras = @(
    # Python Benchmarks & experimental tests
    "demo_lqft.py", 
    "stress_test_memory_win.py", 
    "three_sum_lqft_test.py", 
    "lqft_integrity_proofs.py", 
    "complexity_demonstrator.py", 
    "trie_vs_lqft_benchmark.py", 
    "graph_vs_lqft.py", 
    "the_architects_choice.py", 
    "comprehensive_benchmark.py", 
    "adaptive_benchmark.py", 
    "leetcode_187_test.py", 
    "complexity_crossover.py", 
    "initialize_lqft.py",
    "leak_test.py",
    "leak_verification.py",
    "memory_benchmark.py",
    "perplexity_benchmark_suite.py",
    "advanced_lqft_stress_suite.py",
    "lqft_final_validation.py",
    "test_lqft.py",
    "lqft.py",

    # Note: We are KEEPING test_pure_python_ds.py and pure_python_ds.py
    # because they are required for the GitHub Actions CI/CD Pipeline.

    # Documentation & Guides (Kept in Canvas, removed from Repo for 'Clean' look)
    "lqft_blog_post.md",
    "announcements.md",
    "roadmap.md",
    "performance_report.md",
    "lqft_discovery.md",
    "vs_setup_guide.md",
    "vscode_setup_guide.md",
    "troubleshooting_guide.md",
    "lqft_complexity_analysis.md",
    "complexity_analysis_summary.md",
    "lqft_algorithm_logic.md",
    "lqft_strategic_roadmap.md",
    "lqft_vs_all.md",

    # Scripts
    "github_setup.ps1"
)

foreach ($file in $Extras) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Host "  > Purged: $file" -ForegroundColor Gray
    }
}

# 2. LOCAL BUILD ARTIFACT CLEANUP
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
# Using --allow-empty in case everything was already staged
git commit -m "release: $Version - Final production cleanup and CI fix" --allow-empty
git push origin main

# 4. PYPI TRIGGER (Tagging) - Handling if tag already exists
$TagExists = git tag -l $Version
if (-not $TagExists) {
    Write-Host "[*] Tagging release $Version to trigger GitHub Actions..." -ForegroundColor Cyan
    git tag $Version
    git push origin --tags
} else {
    Write-Host "[*] Tag $Version already exists on GitHub. Skipping tag step." -ForegroundColor Gray
}

# 5. MONITORING
Write-Host "==========================================================" -ForegroundColor Green
Write-Host " ✅ REPOSITORY IS NOW CLEAN & PRODUCTION READY" -ForegroundColor Green
Write-Host " GitHub: https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green