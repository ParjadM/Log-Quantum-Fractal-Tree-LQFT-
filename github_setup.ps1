# LQFT Repository Deployment Script
# Architect: Parjad Minooei

Write-Host "🚀 Initializing LQFT Repository..." -ForegroundColor Pink

# 1. Initialize Git
git init

# 2. Create the .gitignore (Ensures we don't upload the 46KB .pyd binary)
# [The .gitignore file generated above should be saved as .gitignore]

# 3. Add all files
git add .

# 4. Initial Commit
git commit -m "feat: Initial release of LQFT C-Engine V4.1 with O(1) Search and Merkle-Folding"

# 5. Branch Naming
git branch -M main

# 6. Set Remote (Using your provided GitHub profile)
$RepoName = "LQFT-Core-Engine"
Write-Host "[*] Targeting https://github.com/Parja# LQFT Repository Deployment Script
# Architect: Parjad Minooei
# Target: https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-

Clear-Host
Write-Host "==========================================================" -ForegroundColor Pink
Write-Host " 🚀 DEPLOYING: Log-Quantum-Fractal-Tree-LQFT- " -ForegroundColor Pink
Write-Host "==========================================================" -ForegroundColor Pink

# 1. Initialize Git if not already present
if (!(Test-Path .git)) {
    Write-Host "[*] Initializing new Git repository..." -ForegroundColor Cyan
    git init
} else {
    Write-Host "[!] Git repository already initialized." -ForegroundColor Yellow
}

# 2. Safety Check for .gitignore
if (!(Test-Path .gitignore)) {
    Write-Host "[!] WARNING: .gitignore not found. Creating a default to protect binaries..." -ForegroundColor Yellow
    @'
__pycache__/
*.pyd
*.so
build/
dist/
*.egg-info/
.vscode/
.idea/
'@ | Out-File -FilePath .gitignore -Encoding utf8
}

# 3. Prepare README.md
Write-Host "[*] Preparing documentation..." -ForegroundColor Cyan
if (!(Test-Path README.md)) {
    echo "# Log-Quantum-Fractal-Tree-LQFT-" >> README.md
}

# 4. Stage and Commit
Write-Host "[*] Staging files and committing..." -ForegroundColor Cyan
git add .
git commit -m "feat: Initial release of LQFT C-Engine V4.1 with O(1) Search and Merkle-Folding"

# 5. Branch and Remote Configuration
Write-Host "[*] Configuring remote origin..." -ForegroundColor Cyan
git branch -M main

# Check if origin already exists, if so, remove it to avoid conflicts
$remoteExists = git remote | Select-String "origin"
if ($remoteExists) {
    git remote remove origin
}

git remote add origin https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-.git

# 6. Final Push
Write-Host "`n[!] FINAL STEP: Attempting to push to GitHub..." -ForegroundColor Yellow
Write-Host "----------------------------------------------------------"
git push -u origin main

Write-Host "`n✅ Deployment process complete." -ForegroundColor Green
Write-Host "View your repo at: https://github.com/ParjadM/Log-Quantum-Fractal-Tree-LQFT-" -ForegroundColor CyandM/$RepoName" -ForegroundColor Red
git remote add origin "https://github.com/ParjadM/$RepoName.git"

Write-Host "`n[!] ACTION REQUIRED:" -ForegroundColor Yellow
Write-Host "1. Create a new PRIVATE or PUBLIC repository on GitHub named '$RepoName'."
Write-Host "2. Then run the following command to push:"
Write-Host "   git push -u origin main" -ForegroundColor Green