# Documentation Index

## Read This First

Start here based on what you want to do:

### 🚀 **Just want to use it in a new project?**
→ Read **[QUICKSTART.md](QUICKSTART.md)**

Step-by-step walkthrough of:
1. Installing zarr_fuse + dashboard
2. Understanding your data
3. Creating schema files
4. Creating endpoints config
5. Running the dashboard

### 📋 **Want a template to copy/paste?**
→ Use **[TEMPLATE.md](TEMPLATE.md)**

Ready-to-use files you can copy:
- `.env` template
- `config/endpoints.yaml` template
- `schemas/my_schema.yaml` template
- Copy-paste setup commands

### 🔄 **Want the complete workflow?**
→ Read **[WORKFLOW.md](WORKFLOW.md)**

Visual workflow from start to finish:
1. Planning phase (what you need to know)
2. Setup phase (step-by-step)
3. Validation phase (verify everything)
4. Run phase (start dashboard)
5. Troubleshooting quick reference

### 📦 **Need to deploy to production?**
→ Read **[DEPLOYMENT.md](DEPLOYMENT.md)**

Production setup including:
- Docker containers
- Gunicorn servers
- Environment management
- Multiple instances
- Health checks

### ⚙️ **Want to understand config packaging?**
→ Read **[CONFIG_PACKAGING.md](CONFIG_PACKAGING.md)**

Technical details about:
- What files get packaged when installed
- How path resolution works
- Required directory structure
- Testing the package installation

### 🔧 **Developer refactor details?**
→ Read **[REFACTOR_NOTES.md](REFACTOR_NOTES.md)**

Summary of all changes made for plug-and-play:
- Files modified
- Fixes applied
- Backward compatibility notes
- Testing checklist

---

## Quick Decision Tree

```
Do you want to...
│
├─ Use dashboard in a new project?
│  └─→ QUICKSTART.md → TEMPLATE.md
│
├─ Deploy to production?
│  └─→ DEPLOYMENT.md
│
├─ Understand the configuration?
│  └─→ CONFIG_PACKAGING.md
│
├─ See the complete workflow?
│  └─→ WORKFLOW.md
│
└─ Understand the refactor?
   └─→ REFACTOR_NOTES.md
```

---

## Main Documentation

### README.md
Main dashboard documentation. Read this for:
- What the dashboard is
- Installation options (monorepo vs standalone)
- Environment variable configuration
- Troubleshooting

### QUICKSTART.md ⭐ START HERE
Complete guide for using in a new project. Read this for:
- Step-by-step setup (8 steps)
- Understanding your data
- Creating schema files
- Creating endpoints config
- Testing your setup
- Deployment options
- Troubleshooting by error

### TEMPLATE.md
Copy-paste templates and checklist. Use this to:
- Create .env file
- Create config/endpoints.yaml
- Create schemas/my_schema.yaml
- Verify everything works
- Quick setup commands

### WORKFLOW.md
Complete workflow visualization. Read this for:
- Overview of the process
- Planning phase questions
- Setup phase commands
- Validation phase tests
- Troubleshooting quick reference
- Success indicators

### DEPLOYMENT.md
Production deployment guide. Read this for:
- PyPI installation
- Docker setup
- Gunicorn configuration
- Using custom data sources
- Multiple endpoints
- Performance tips

### CONFIG_PACKAGING.md
Technical packaging details. Read this for:
- What gets packaged where
- Path resolution logic
- Directory structure requirements
- Installation verification

### REFACTOR_NOTES.md
Technical refactor summary. Read this for:
- All changes made
- Files modified
- Improvements summary
- Testing checklist

---

## Getting Help

### Common Issues

**"Where do I start?"**
→ Start with [QUICKSTART.md](QUICKSTART.md)

**"What files do I need to create?"**
→ Copy templates from [TEMPLATE.md](TEMPLATE.md)

**"Something went wrong"**
→ Check troubleshooting in [QUICKSTART.md](QUICKSTART.md) or [WORKFLOW.md](WORKFLOW.md)

**"How do I deploy?"**
→ Read [DEPLOYMENT.md](DEPLOYMENT.md)

**"I want to understand the config"**
→ Read [CONFIG_PACKAGING.md](CONFIG_PACKAGING.md)

---

## File Organization

```
dashboard/
├── README.md                    ← What is this?
├── QUICKSTART.md               ← START HERE for new projects ⭐
├── TEMPLATE.md                 ← Copy-paste templates
├── WORKFLOW.md                 ← Complete workflow
├── DEPLOYMENT.md               ← Production setup
├── CONFIG_PACKAGING.md         ← Technical details
├── REFACTOR_NOTES.md          ← What changed
├── .env.example                ← Env var template
├── pyproject.toml              ← Package metadata
├── config/
│   ├── endpoints.yaml          ← Default endpoints config
│   └── dashboard_config.py     ← Config parsing
├── schemas/
│   └── bukov_schema.yaml       ← Example schema
├── [source files]              ← Dashboard code
└── test/
    └── [tests]                 ← Tests
```

---

## Recommended Reading Order

1. **First time?** Read in this order:
   - README.md (overview)
   - QUICKSTART.md (step-by-step)
   - TEMPLATE.md (copy templates)

2. **Deploying?** Read:
   - QUICKSTART.md (to understand)
   - DEPLOYMENT.md (for production)

3. **Troubleshooting?** Read:
   - WORKFLOW.md (troubleshooting section)
   - Check error messages in QUICKSTART.md

4. **Advanced?** Read:
   - CONFIG_PACKAGING.md (understand packaging)
   - REFACTOR_NOTES.md (understand changes)

---

## TL;DR = The Absolute Minimum

```bash
# 1. Install
pip install zarr-fuse zarr-fuse-dashboard

# 2. Create structure
mkdir config schemas

# 3. Create config/endpoints.yaml (customize for your data)
# See TEMPLATE.md

# 4. Create schemas/my_schema.yaml (describe your Zarr structure)
# See TEMPLATE.md

# 5. Create .env (set your data location)
# See TEMPLATE.md

# 6. Run
export ENDPOINTS_PATH=$(pwd)/config/endpoints.yaml
zf-dashboard
```

That's it! For details, see QUICKSTART.md

---

## Document History

| File | Purpose | Created |
|------|---------|---------|
| README.md | Main docs | Original |
| QUICKSTART.md | New project setup | Refactor v2 |
| TEMPLATE.md | Copy-paste templates | Refactor v2 |
| WORKFLOW.md | Visual workflow | Refactor v2 |
| DEPLOYMENT.md | Production setup | Refactor v1 |
| CONFIG_PACKAGING.md | Technical details | Refactor v2 |
| REFACTOR_NOTES.md | Change summary | Refactor v2 |

---

**Questions? Start with README.md, then go to QUICKSTART.md**

**Ready to go? Use TEMPLATE.md and follow WORKFLOW.md**

