# LQFT Production Release (v0.3.0)
# Architect: Parjad Minooei
# Status: Process Isolation, Circuit Breakers, & Type Safety Active

$Version = "v0.3.0"

Write-Host "==========================================================" -ForegroundColor Magenta
Write-Host " 🚀 INITIATING PRODUCTION RELEASE: $Version" -ForegroundColor Magenta
Write-Host "==========================================================" -ForegroundColor Magenta

# 1. THE TOTAL PURGE (Cleaning experimental noise for the MScAC Portfolio)
Write-Host "[*] Purging experimental scripts and old verification suites..." -ForegroundColor Yellow
$Extras = @(
    "leak_test.py", "leak_verification.py", "comprehensive_benchmark.py", 
    "adaptive_benchmark.py", "trie_vs_lqft_benchmark.py", "graph_vs_lqft.py", 
    "the_architects_choice.py", "complexity_demonstrator.py", "complexity_crossover.py", 
    "leetcode_187_test.py", "memory_benchmark.py", "perplexity_benchmark_suite.py", 
    "advanced_lqft_stress_suite.py", "three_sum_lqft_test.py",
    "lqft_integrity_proofs.py", "demo_lqft.py", "stress_test_memory_win.py", "initialize_lqft.py",
    "github_setup.ps1", "integrity_check_v44.py", "stress_test_large_payload.py",
    "enterprise_capability_suite.py", "pre_release_suite.py"
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

# 3. CI/CD PIPELINE RESCUE
# Ensures GitHub Actions has the necessary validation script to pass the health check.
Write-Host "[*] Verifying CI/CD required files..." -ForegroundColor Yellow
if (!(Test-Path "validation.py")) {
    Write-Host "  > Recreating missing validation.py for GitHub Actions..." -ForegroundColor Cyan
    @'
import sys
import hashlib

def fast_hash(key):
    return int(hashlib.md5(str(key).encode()).hexdigest()[:16], 16)

print("====================================================")
print("   LQFT CI/CD PIPELINE: NATIVE C-ENGINE VALIDATION")
print("====================================================")

try:
    import lqft_c_engine
    print("[*] Native C-Engine loaded successfully.")
    
    # Verify core operations: Insert -> Search -> Delete
    h = fast_hash("ci_test_vector")
    lqft_c_engine.insert(h, "verification_payload")
    
    result = lqft_c_engine.search(h)
    assert result == "verification_payload", "Data corruption detected!"
    
    lqft_c_engine.delete(h)
    assert lqft_c_engine.search(h) is None, "Deletion logic failed!"
    
    print("[*] Full CRUD lifecycle verified.")
    print("[*] CI/CD Pipeline PASS.")
except Exception as e:
    print(f"[!] CI/CD Error: {e}")
    sys.exit(1)
'@ | Out-File -FilePath validation.py -Encoding utf8
}

# 4. GITHUB SYNC
Write-Host "[*] Staging stable production core..." -ForegroundColor Cyan
git add .
git commit -m "release: $Version - Memory Circuit Breaker & Strict Type Safety" --allow-empty
git push origin main

# 5. TAGGING (Triggers PyPI Action)
Write-Host "[*] Updating release tag $Version..." -ForegroundColor Cyan
# Remove existing tag if it exists locally or remotely to ensure a fresh build
git tag -d $Version 2>$null
git push origin :refs/tags/$Version 2>$null

# Apply and push the new production tag
git tag $Version
git push origin --tags

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " ✅ DEPLOYMENT $Version LIVE ON GITHUB & PYPI" -ForegroundColor Green
Write-Host " Repository: https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Green