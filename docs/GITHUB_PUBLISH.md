# Publish SOG to GitHub

Use this checklist to publish the repository so others can clone and run it.

## 1) Create a New Repository on GitHub
On GitHub:
1. Click `New repository`
2. Name it (example: `SOG`)
3. Keep it empty (do not add README/license/gitignore from GitHub UI)
4. Create repository

Copy the HTTPS URL, for example:
- `https://github.com/MaTaha-ualr/Synthetic-Occupancy-Generator-data_pipeline.git`

## 2) Initialize Git Locally
Run inside the local `SOG` folder:
```bash
git init
git branch -M main
```

## 3) Stage Files
```bash
git add .
```

Check what will be committed:
```bash
git status
```

## 4) First Commit
```bash
git commit -m "Initial commit: SOG Phase-1 pipeline and docs"
```

## 5) Connect Remote and Push
```bash
git remote add origin https://github.com/MaTaha-ualr/Synthetic-Occupancy-Generator-data_pipeline.git
git push -u origin main
```

If prompted, authenticate with your GitHub account/token.

## 6) Verify On GitHub
Open your repository page and confirm these files are visible:
- `README.md`
- `docs/BEGINNER_GUIDE.md`
- `configs/phase1.yaml`
- `scripts/build_prepared.py`
- `scripts/generate_phase1.py`

## 7) License
The repository already includes an MIT `LICENSE` file.

## 8) Optional: Mark a First Release
```bash
git tag v1.0.0
git push origin v1.0.0
```

## 9) Optional: Keep Docs Synced
If the repository URL changes in the future, update the clone/remote commands in `README.md` and this document.
