# GitHub Push Instructions

## Prerequisites

### Step 1: Install Git
**Windows**: Download from [git-scm.com](https://git-scm.com/download/win)
- Run the installer and use default settings
- Restart your terminal/VS Code after installation

### Step 2: Create GitHub Account (if you don't have one)
- Go to [github.com](https://github.com)
- Sign up for a free account
- Verify your email

### Step 3: Create a New Repository on GitHub
1. Log in to GitHub
2. Click **+** (top right) → **New repository**
3. Repository name: `data-engineering-portfolio` (or your preferred name)
4. Description: `Production-grade data engineering projects showcasing enterprise-scale systems`
5. Set to **Public** (for portfolio showcase)
6. **Do NOT** initialize with README/gitignore (we have them)
7. Click **Create repository**

---

## Push to GitHub (from PowerShell)

After creating the repository, GitHub will show you these commands. Run them in PowerShell:

### Option A: HTTPS (Simpler - Recommended for First Time)

```powershell
cd "c:\Users\LENOVO\Downloads\Resume Data engineering"
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
git init
git add .
git commit -m "Initial commit: Production-grade data engineering portfolio with 5 enterprise projects"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/data-engineering-portfolio.git
git push -u origin main
```

**First time only**: You'll be prompted to authenticate. Click "Authorize" in the browser popup.

### Option B: SSH (More Secure - Recommended for Ongoing Work)

#### Setup SSH Key (one-time):
```powershell
# Generate SSH key
ssh-keygen -t ed25519 -C "your.email@example.com"
# Press Enter 3 times to accept defaults

# Copy the public key
Get-Content $env:USERPROFILE\.ssh\id_ed25519.pub | Set-Clipboard
```

Then:
1. Go to GitHub → Settings → SSH and GPG keys → New SSH key
2. Paste the key and save

#### Push with SSH:
```powershell
cd "c:\Users\LENOVO\Downloads\Resume Data engineering"
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
git init
git add .
git commit -m "Initial commit: Production-grade data engineering portfolio with 5 enterprise projects"
git branch -M main
git remote add origin git@github.com:YOUR_USERNAME/data-engineering-portfolio.git
git push -u origin main
```

---

## What Gets Pushed

✅ **Included**:
- All 5 project directories with complete code
- Configuration files (requirements.txt, docker-compose.yml, terraform)
- Documentation (README, architecture guides, technical specs)
- `.gitignore` (prevents sensitive data)

❌ **Excluded** (by .gitignore):
- Python cache (__pycache__)
- Virtual environments (venv/)
- Logs and temporary files
- Credentials and .env files
- Large data files

---

## After First Push

### Add Future Changes

```powershell
cd "c:\Users\LENOVO\Downloads\Resume Data engineering"

# See what changed
git status

# Add specific files
git add path/to/file.py

# Or add all changes
git add .

# Commit
git commit -m "Add feature description"

# Push
git push
```

### View Your Portfolio Online

After pushing:
- Visit: `https://github.com/YOUR_USERNAME/data-engineering-portfolio`
- Share this link for interviews, portfolio sites, LinkedIn

---

## Troubleshooting

### "git: command not found"
- Git not installed or not in PATH
- Restart terminal after installing Git
- Try PowerShell instead of CMD

### "Permission denied" (SSH)
- SSH key not added to GitHub
- Try HTTPS method instead

### "fatal: not a git repository"
- Run `git init` in the project folder first

### "Authentication failed" (HTTPS)
- Use Personal Access Token instead of password
- Generate at: GitHub → Settings → Developer settings → Personal access tokens

---

## Resume Integration Tips

Add to your resume/portfolio:

```
Data Engineering Portfolio
https://github.com/YOUR_USERNAME/data-engineering-portfolio

5 Production-Grade Projects:
• Real-Time Streaming Platform (Kafka, Spark Streaming, backpressure, circuit breakers)
• Enterprise Data Warehouse (Snowflake, SCD Type 2, Airflow, dbt)
• Data Quality Framework (Great Expectations, Prometheus, anomaly detection)
• Data Lakehouse (Delta Lake, Medallion architecture, Z-Order clustering)
• Fraud Detection Pipeline (Ensemble ML, feature store, SHAP explainability)

4,350+ lines of code | 5 advanced architecture guides
```

---

## Quick Reference

| Action | Command |
|--------|---------|
| Check status | `git status` |
| View commits | `git log --oneline` |
| View diff | `git diff` |
| Undo last commit | `git reset --soft HEAD~1` |
| Switch branch | `git checkout branch-name` |
| Create branch | `git checkout -b new-feature` |

