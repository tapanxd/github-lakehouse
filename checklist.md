# GitHub Lakehouse — Project Checklist

## ✅ Done
- [x] Azure resource group (guthub-lakehouse-rg)
- [x] ADLS Gen2 storage account (ghlakehousestorage) + 4 containers
- [x] Key Vault (gh-lakehouse-kv) + secret
- [x] Databricks workspace + cluster
- [x] Databricks secret scope linked to Key Vault
- [x] ADLS connection tested from Databricks
- [x] GH Archive file uploaded and schema validated (156k events)
- [x] Repo cleaned up from template
- [x] databricks.yml
- [x] src/gh_archive/imports.py
- [x] src/gh_archive/bronze_autoloader.py
- [x] src/gh_archive/silver_dlt.py
- [x] src/gh_archive/gold_dlt.py

---

## 🔲 Code — Still to Build
- [ ] src/cost_monitor/alert.py
- [ ] src/tests/test_transformations.py (5 pytest tests)
- [ ] src/custom_datasource/gh_archive_ingest.py (local download + ADLS upload script)

---

## 🔲 Databricks Configuration
- [ ] Run bronze_autoloader.py end-to-end in Databricks
- [ ] Set up DLT Silver pipeline in Databricks UI (or via DAB deploy)
- [ ] Set up DLT Gold pipeline in Databricks UI (or via DAB deploy)
- [ ] Verify Silver table populated correctly
- [ ] Verify Gold tables populated correctly
- [ ] Create Databricks SQL dashboard (4 tiles):
  - [ ] Events per hour (line chart)
  - [ ] Top repos by stars today (bar chart)
  - [ ] Cost per pipeline run (table)
  - [ ] Idle DBU wasted this week (single number)

---

## 🔲 Resource YMLs
- [ ] resources/dlt_silver.pipeline.yml (point to silver_dlt.py)
- [ ] resources/dlt_gold.pipeline.yml (point to gold_dlt.py)
- [ ] resources/deploy.job.yml (orchestration job)
- [ ] resources/database.yml (schema definitions)
- [ ] resources/volumes.yml (ADLS volume mounts)
- [ ] resources/schemas.yml (uncomment and adapt)

---

## 🔲 GitHub Actions Workflows
- [ ] .github/workflows/ci.yml (pytest on every PR)
- [ ] .github/workflows/fetch_gharchive.yml (hourly cron fetch + upload)
- [ ] .github/workflows/terraform_plan.yml (terraform plan on PR)
- [ ] Add GitHub secrets:
  - [ ] AZURE_STORAGE_KEY
  - [ ] AZURE_STORAGE_ACCOUNT
  - [ ] DATABRICKS_TOKEN
  - [ ] ARM_CLIENT_ID
  - [ ] ARM_CLIENT_SECRET
  - [ ] ARM_TENANT_ID
  - [ ] ARM_SUBSCRIPTION_ID

---

## 🔲 Terraform
- [ ] terraform/main.tf
- [ ] terraform/variables.tf
- [ ] terraform/outputs.tf
- [ ] Import existing resources (resource group, storage, Key Vault, Databricks)
- [ ] Cluster policy (autoscaling 1-3, auto-terminate 20 min)
- [ ] Replace plaintext storage key in cluster Spark config with Key Vault reference

---

## 🔲 Repo Polish
- [ ] README.md (architecture diagram, setup instructions, known limitations)
- [ ] Architecture diagram (Excalidraw → PNG)
- [ ] requirements.txt updated with all dependencies
- [ ] .gitignore covers .env, __pycache__, .databricks/bundle/

---

## 🔲 Resume
- [ ] Replace 40% placeholder with real cost monitor number
- [ ] Add project to resume under Projects section
- [ ] Push repo to GitHub (public)
