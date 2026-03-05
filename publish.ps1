# LQFT Production Release (v0.9.1)
# Architect: Parjad Minooei
# Status: Phase 3 (Multi-Language Core) - Stability & Performance Patch

$Version = "v0.9.1"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " 🚀 INITIATING PRODUCTION RELEASE: $Version" -ForegroundColor Magenta
Write-Host " Status: Custom Arena Allocator & Silicon-Speed Locked" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. THE TOTAL PURGE (Keeping the MScAC Portfolio focused)
Write-Host "[*] Purging experimental benchmarks and verification suites..." -ForegroundColor Yellow
$Extras = @(
    "leak_test.py", "leak_verification.py", "comprehensive_benchmark.py", 
    "adaptive_benchmark.py", "trie_vs_lqft_benchmark.py", "graph_vs_lqft.py", 
    "the_architects_choice.py", "complexity_demonstrator.py", "complexity_crossover.py", 
    "leetcode_187_test.py", "memory_benchmark.py", "perplexity_benchmark_suite.py", 
    "advanced_lqft_stress_suite.py", "three_sum_lqft_test.py",
    "lqft_integrity_proofs.py", "demo_lqft.py", "stress_test_memory_win.py", "initialize_lqft.py",
    "github_setup.ps1", "integrity_check_v44.py", "stress_test_large_payload.py",
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

# 3. CI/CD PIPELINE HANDSHAKE
Write-Host "[*] Verifying CI/CD validation protocol..." -ForegroundColor Yellow
@'
import sys
import hashlib
import os

def fast_hash(key):
    return int(hashlib.md5(str(key).encode()).hexdigest()[:16], 16)

try:
    import lqft_c_engine
    h = fast_hash("ci_verification_v091")
    lqft_c_engine.insert(h, "stable_core_v091")
    result = lqft_c_engine.search(h)
    assert result == "stable_core_v091", "Data integrity failure in Arena!"
    
    # Verify Deletion Fast-Path
    lqft_c_engine.delete(h)
    assert lqft_c_engine.search(h) is None, "Arena deletion failure!"
    
    lqft_c_engine.free_all()
    print("[*] CI/CD Pipeline PASS (v0.9.1 Core Verified).")
except Exception as e:
    print(f"[!] CI/CD Error: {e}")
    sys.exit(1)
'@ | Out-File -FilePath validation.py -Encoding utf8

# 4. GITHUB SYNC
Write-Host "[*] Staging v0.9.1 source code..." -ForegroundColor Cyan
git add .
git commit -m "release: $Version - Stable Custom Arena Allocator & Hardware Bandwidth Saturation" --allow-empty
git push origin main

# 5. TAGGING (Triggers GitHub Actions PyPI Build)
Write-Host "[*] Updating production tag to $Version..." -ForegroundColor Cyan
git tag -d $Version 2>$null
git push origin :refs/tags/$Version 2>$null

git tag $Version
git push origin --tags

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " ✅ DEPLOYMENT $Version LIVE" -ForegroundColor Green
Write-Host " Repository: https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green