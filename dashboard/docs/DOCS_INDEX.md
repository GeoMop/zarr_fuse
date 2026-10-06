# Documentation Index

## Read This First

Start here based on what you want to do:

### 🚀 **Just want to use it in a new project?**
→ Read **[TEMPLATE.md](TEMPLATE.md)**

Complete guide covering:
1. Planning phase (what you need to know)
2. Template files (.env, zf_view.yaml, my_schema.yaml)
3. Validation phase (verify everything)
4. Running the dashboard
5. Troubleshooting quick reference
6. Deployment overview (development, gunicorn, Docker)

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

---

## Quick Decision Tree

```
Do you want to...
│
├─ Use dashboard in a new project?
│  └─→ TEMPLATE.md
│
├─ Deploy to production?
│  └─→ DEPLOYMENT.md
│
├─ Understand the configuration?
│  └─→ CONFIG_PACKAGING.md
│
```

---

## Main Documentation

### README.md
Main dashboard documentation. Read this for:
- What the dashboard is
- Installation options (monorepo vs standalone)
- Environment variable configuration
- Troubleshooting

### TEMPLATE.md ⭐ START HERE
Complete guide for using the dashboard in a new project. Read this for:
- Planning phase (what you need to know)
- Template files (.env, config/zf_view.yaml, schemas/my_schema.yaml)
- Copy-paste setup commands
- Validation phase tests and verification checklist
- Running the dashboard
- Troubleshooting by error
- Customizations
- Deployment overview (development, gunicorn, Docker)

### DEPLOYMENT.md
Production deployment guide. Read this for:
- PyPI installation
- Docker setup
- Gunicorn configuration
- Using custom data sources
- Multiple views
- Performance tips

### CONFIG_PACKAGING.md
Technical packaging details. Read this for:
- What gets packaged where
- Path resolution logic
- Directory structure requirements
- Installation verification

---

## Getting Help

### Common Issues

**"Where do I start?"**
→ Start with [TEMPLATE.md](TEMPLATE.md)

**"What files do I need to create?"**
→ Copy templates from [TEMPLATE.md](TEMPLATE.md)

**"Something went wrong"**
→ Check the troubleshooting section in [TEMPLATE.md](TEMPLATE.md)

**"How do I deploy?"**
→ Read [DEPLOYMENT.md](DEPLOYMENT.md)

**"I want to understand the config"**
→ Read [CONFIG_PACKAGING.md](CONFIG_PACKAGING.md)

---

## File Organization

```
dashboard/
├── README.md                    ← What is this?
├── TEMPLATE.md                  ← START HERE for new projects ⭐ (templates + workflow)
├── DEPLOYMENT.md                ← Production setup
├── CONFIG_PACKAGING.md          ← Technical details
├── .env.example                ← Env var template
├── pyproject.toml              ← Package metadata
├── config/
│   ├── zf_view.yaml          ← Default views config
│   └── [runtime config assets]
├── config.py                   ← Config parsing
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
   - TEMPLATE.md (planning, templates, setup, troubleshooting)

2. **Deploying?** Read:
   - TEMPLATE.md (to understand setup)
   - DEPLOYMENT.md (for production)

3. **Troubleshooting?** Read:
   - TEMPLATE.md (troubleshooting section)

4. **Advanced?** Read:
   - CONFIG_PACKAGING.md (understand packaging)

---

## TL;DR = The Absolute Minimum

```bash
# 1. Install
pip install zarr-fuse zarr_fuse.dashboard

# 2. Create structure
mkdir config schemas

# 3. Create config/zf_view.yaml (customize for your data)
# See TEMPLATE.md

# 4. Create schemas/my_schema.yaml (describe your Zarr structure)
# See TEMPLATE.md

# 5. Create .env (set your data location)
# See TEMPLATE.md

# 6. Run
export ZF_VIEW_PATH=$(pwd)/config/zf_view.yaml
zf-dashboard
```

That's it! For details, see TEMPLATE.md

---

## Document History

| File | Purpose | Created |
|------|---------|---------|
| README.md | Main docs | Original |
| TEMPLATE.md | New project setup: templates + workflow (QUICKSTART + WORKFLOW merged) | Refactor v2 |
| DEPLOYMENT.md | Production setup | Refactor v1 |
| CONFIG_PACKAGING.md | Technical details | Refactor v2 |

> QUICKSTART.md and WORKFLOW.md were merged into TEMPLATE.md (single new-project guide).

---

**Questions? Start with README.md, then go to TEMPLATE.md**

