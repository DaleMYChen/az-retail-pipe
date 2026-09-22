### Project outline & components
- Azure Data Factory for orchestration (= Airflow);
- Databricks for ELT + internal task chaining (= dbt DAG, one layer down);
- Power BI for analytics;
- Azure DevOps for CI/CD (Repos + one YAML pipeline).

# retail-azure-de-project

Faker-generated retail data, ingested and transformed on Azure, mirroring the *intent* of the
BigQuery/dbt/Airflow version — but built the way Azure DE actually works, not forced into the old shape.

Flow: **ADF (orchestrate) → Databricks (transform, medallion) → Power BI (report)**, with
**Azure DevOps (Repos + one YAML pipeline)** wrapping the whole thing in CI/CD.

---

## 0. Environment map

**Inside resource group `rg-retail-de`:**
- **Storage account** (`retaildesadc`) → ADLS Gen2, containers `raw`, `bronze`, `silver`, `gold`, `catalog-root`
- **Data Factory** (`adf-retail-de-dc`) → holds pipelines (orchestration definitions), not data
- **Databricks workspace** (`dbw-retail-de`, Premium — Standard is retired) → own web UI, holds notebooks/jobs/Unity Catalog

**Separate SaaS, outside the resource group:**
- **Power BI** (powerbi.com) → connects into Databricks as a data source
- **Azure DevOps** (`dev.azure.com/retail-de-lab`) → Repos (git) + Pipelines (CI/CD), acts *on* the subscription via identity, not inside it

**End-to-end object flow:**
```
Faker CSV (laptop) → az storage blob upload-batch → ADLS /raw → UC volume (retail_de.raw.landing)
   → Databricks Job (bronze → silver_scd2 → silver_transform → gold), triggered by ADF
   → Power BI, via Databricks SQL Warehouse
```

**Three orchestration/flow layers, kept distinct on purpose:**
1. **Data flow** — CSV → ADLS → Delta bronze/silver/gold → Power BI query
2. **Orchestration flow** (nested, two layers):
   - Layer 1: ADF pipeline triggers the Databricks **Job** as one unit (ADF doesn't see inside it)
   - Layer 2: the Job's own tasks, chained by **Databricks Workflows** — this is the dbt-DAG-equivalent, except explicit dependency declarations instead of `ref()`-inferred ones
3. **CI/CD flow** — Azure DevOps Repos holds ADF pipeline JSON + Databricks notebook source; a YAML pipeline deploys both

Bronze/silver/gold are **notebooks**, not repos — Repos is just the git-sync mechanism.

---

## 1. Setup — Azure resources (Cloud Shell)

```bash
az account list --output table
az account set --subscription "<subscription-id-or-name>"

az group create --name rg-retail-de --location australiaeast

az storage account create \
  --name retaildesadc --resource-group rg-retail-de --location australiaeast \
  --sku Standard_LRS --kind StorageV2 --hierarchical-namespace true

for c in raw bronze silver gold catalog-root; do
  az storage container create --account-name retaildesadc --name $c --auth-mode login
done

az datafactory create --resource-group rg-retail-de --factory-name adf-retail-de-dc

az databricks workspace create \
  --resource-group rg-retail-de --name dbw-retail-de \
  --location australiaeast --sku premium   # Standard SKU retired

az resource list --resource-group rg-retail-de --output table   # verify
```

---

## 2. Data generation

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python data_generator/generate_raw_data.py --out-dir data
```
7 CSVs → Cloud Shell upload → push to `/raw`:
```bash
az storage blob upload-batch \
  --account-name retaildesadc --destination raw \
  --source ~ --pattern "*.csv" --auth-mode login
```

---

## 3. Unity Catalog setup

**3.1 Access Connector** (managed identity Databricks uses to reach storage):
```bash
az databricks access-connector create \
  --resource-group rg-retail-de --name ac-retail-de \
  --location australiaeast --identity-type SystemAssigned
```

**3.2 Grant it storage access:**
```bash
CONNECTOR_ID=$(az databricks access-connector show --resource-group rg-retail-de --name ac-retail-de --query id -o tsv)
PRINCIPAL_ID=$(az databricks access-connector show --resource-group rg-retail-de --name ac-retail-de --query identity.principalId -o tsv)

az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee-object-id "$PRINCIPAL_ID" --assignee-principal-type ServicePrincipal \
  --scope "/subscriptions/$(az account show --query id -o tsv)/resourceGroups/rg-retail-de/providers/Microsoft.Storage/storageAccounts/retaildesadc"
```

**3.3 In Databricks workspace (Launch Workspace from the portal resource page) → Catalog:**
- **Create credential**: `cred-retail-de`, Azure Managed Identity, access connector ID from step 3.1
- Note: Azure now **auto-creates a metastore** on first workspace — no manual metastore step needed. The auto catalog `dbw_retail_de` has no storage root; don't use it for this project's tables.
- **Create external location** `ext-catalog-root` → `abfss://catalog-root@retaildesadc.dfs.core.windows.net/` → credential `cred-retail-de`. Force-create through the "File Events" warning — not needed, that's for Auto Loader.

**3.4 Create catalog, with explicit managed location** (auto metastore has no default storage, so this is required, not optional):
```sql
CREATE CATALOG IF NOT EXISTS retail_de
MANAGED LOCATION 'abfss://catalog-root@retaildesadc.dfs.core.windows.net/';
```

**3.5 External locations + schemas** for each layer, same credential:
```
ext-raw      abfss://raw@retaildesadc.dfs.core.windows.net/
ext-bronze   abfss://bronze@retaildesadc.dfs.core.windows.net/
ext-silver   abfss://silver@retaildesadc.dfs.core.windows.net/
ext-gold     abfss://gold@retaildesadc.dfs.core.windows.net/
```
```sql
CREATE SCHEMA IF NOT EXISTS retail_de.bronze MANAGED LOCATION 'abfss://bronze@retaildesadc.dfs.core.windows.net/';
CREATE SCHEMA IF NOT EXISTS retail_de.silver MANAGED LOCATION 'abfss://silver@retaildesadc.dfs.core.windows.net/';
CREATE SCHEMA IF NOT EXISTS retail_de.gold   MANAGED LOCATION 'abfss://gold@retaildesadc.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS retail_de.raw;
CREATE EXTERNAL VOLUME retail_de.raw.landing
LOCATION 'abfss://raw@retaildesadc.dfs.core.windows.net/';
```
Confirm 5 schemas in Catalog Explorer: `default, information_schema, raw, bronze, silver, gold`.

Why a volume for raw: with UC enabled, standard compute can't read arbitrary `abfss://` paths — only UC-governed ones. Notebooks read `/Volumes/retail_de/raw/landing/customers.csv`, not the raw path directly.

---

## 4. Transformation — Databricks notebooks

All four notebooks live in `notebooks/`, imported via Workspace → Import (later moved under Databricks Repos, see §9).

**`01_bronze_ingest.py`** — read raw CSVs from the volume, cast messy multi-format datetimes (`coalesce` over `try_to_timestamp` across 3 formats), flag (not drop) nulls, write Delta to `retail_de.bronze.*`.

**`02a_silver_scd2.py`** — customers/products only. dbt's snapshot compared *live source* vs *last snapshot*; here bronze itself has no history (overwritten every run), so silver SCD2 compares **this run's bronze snapshot** against **silver's currently-open row** (`is_current = true`). Two-step Delta `MERGE`: close changed rows (`valid_to` = new row's `updated_at`, matching old dbt semantics — not wall-clock), then insert new open versions. First run = pure initial load.

**`02b_silver_transform.py`** — `order_items_validated` (flags negative qty/price, orphaned product FK, keeps all rows), `order_items_priced` (valid lines only — a bad order's total deliberately understates), `orders_enriched` (current-state customer/store join — point-in-time asof join deferred), `payments_validated` (settlement-before-payment flag + lag), `returns_matched` (orphan flag, matched + orphaned both kept).

**`03_gold_aggregate.py`** — `fct_order_items` (straight promotion of `order_items_priced`), `fct_orders` (orders_enriched + item/payment/returns rollups, left joins, coalesced to 0). Sanity checks: no negative `order_net_amount`, row count matches source orders.

Results after first full run: customers 500, products 150, stores 12, orders 3000, order_items 7000 (6541 valid), payments 3000 (143 bad settlement sequence), returns 250 (8 orphaned), fct_orders 3000.

---

## 5. Databricks Workflow chaining

Jobs & Pipelines → Create Job → `retail_de_medallion_pipeline`:
- Task 1 `bronze_ingest` — notebook `bronze_ingest`, Serverless, no dependency
- Task 2 `silver_scd2` — notebook `silver_scd2`, depends on `bronze_ingest`
- Task 3 `silver_transform` — notebook `silver_transform`, depends on `silver_scd2`
- Task 4 `gold_aggregate` — notebook `gold_aggregate`, depends on `silver_transform`

Run Now to verify the chain. **Job ID: `1035721459403107`** — needed by the ADF Job activity.

---

## 6. ADF orchestration

ADF has a native **Databricks "Job" activity** (GA'd from preview mid-2025) — no REST-API workaround needed, it triggers an existing Job by name and awaits completion.

**6.1 Auth: ADF's managed identity → Databricks, no stored secret.**
```bash
az datafactory show --resource-group rg-retail-de --factory-name adf-retail-de-dc --query identity
# principalId: 2f449f4e-845d-4536-a960-d5dece2dc52f

az ad sp show --id 2f449f4e-845d-4536-a960-d5dece2dc52f --query appId -o tsv
# appId: 871c1918-db93-4566-a97e-cb800bad235f
```
Databricks **account console** → User management → Service principals → Add:
- **Microsoft Entra ID managed** (not "Databricks managed" — that creates an unrelated new identity; Entra ID managed links to the *actual* ADF identity via the Application ID above)
- Name `adf-retail-de-dc`, paste the `appId`
- Add it to workspace `dbw-retail-de`

Job permissions (workspace UI, Jobs & Pipelines → job → **Permissions panel from the sidebar, not the ⋯ menu**) → Add principal → search `adf-retail-de-dc` → **Can Manage Run**.

**6.2 Linked service** (ADF Studio → Manage → Linked services → New → Azure Databricks):
```
Name: dbw_retail_de
Integration runtime: AutoResolveIntegrationRuntime
Databricks workspace: dbw-retail-de (from Azure subscription)
Select cluster: Serverless
Authentication type: Managed service identity
→ Test connection → Create
```

**6.3 Pipeline:**
```
New pipeline → name: pl_run_medallion_job
Activities → Databricks → drag "Job" onto canvas → rename activity: medallion_elt
Settings → Databricks linked service: dbw_retail_de → Job: retail_de_medallion_pipeline
Debug → confirm success
```

**⚠ Gotcha**: in Git mode (see §7), the primary action is **Save**, not Publish — Debug alone does *not* persist the pipeline anywhere. Save first (commits to `main`), confirm `adf/pipeline/pl_run_medallion_job.json` actually appears in the repo, *then* Publish. Skipping Save and only Debugging looks fine in the moment but the pipeline vanishes from Author view on reload.

**Status: done** — pipeline saved, `adf/pipeline/pl_run_medallion_job.json` confirmed in repo, published (`adf_publish` branch generated).

---

## 7. Azure DevOps setup

**7.1** Portal → Azure DevOps Organizations → Create org `retail-de-lab` → New project `az-retail-pipe` (auto-creates a Repos git repo of the same name).

**7.2** PAT (org → User settings → Personal access tokens) as push credential. Cache it (Mac):
```bash
git config --global credential.helper osxkeychain
```

**7.3** Push local repo:
```bash
git remote add azure https://dev.azure.com/retail-de-lab/az-retail-pipe/_git/az-retail-pipe
git push azure --all
```

**7.4** Link the org to Entra ID (Organization settings → General → Microsoft Entra → Connect directory). Without this, ADF's Git configuration can't resolve the org/project at all.

**⚠ Gotcha**: `chenmuye5230@gmail.com` is a personal Microsoft Account, not a native Entra ID account — connecting the directory can leave the DevOps UI (`dev.azure.com/me`) in a stuck/looping auth state for a few minutes, looking like the org and project were deleted. They weren't. Confirm via `dev.azure.com/<org>/_projects` directly, or Portal → search "Azure DevOps Organizations", before assuming data loss.

**7.5** Organization Settings → Policies → **allow external guests** (needed since this is a cross-tenant MSA scenario).

**7.6** Connect ADF (Manage → Git configuration):
```
Repository type: Azure DevOps Git
Type: Cloud (cross-tenant)        ← required for MSA/guest scenario, not plain "Cloud"
Organization: retail-de-lab
Project: az-retail-pipe
Repository: az-retail-pipe
Collaboration branch: main
Publish branch: adf_publish (default)
Root folder: /adf
Import existing resources: yes → import into: main
```

**Status: done** — `/adf` folder confirmed in repo (`factory/`, `linkedService/`, `pipeline/`).

---

## 8. Connect Databricks notebooks to DevOps repo

DevOps repo should then contain:
- local clone;
- ADF Git config;
- Databricks repos

, so notebook source is under the same version control as the ADF JSON:

```
Databricks workspace → Repos → Create Git Repo
URL: https://dev.azure.com/retail-de-lab/az-retail-pipe/_git/az-retail-pipe
Provider: Azure DevOps Services
```


Note on ordering: this project built Databricks-first, DevOps-last — the right learning order, but it means the Job was originally wired against loose notebooks that predated the repo, requiring this one-time repoint `/Users/.../az-retail-pipe/databricks_notebooks/bronze_ingest`. 
<br>
A real production setup connects Repos before creating any Job tasks, so this reconciliation step wouldn't exist.

---

## 9. Azure DevOps — YAML pipeline

DevOps yaml pipeline: Automate (on merge) manual Save/Publish in ADF and Commit & Push in Databricks Repos. 

#### 9.1. Create service connection for DevOps to DBW
```
DevOps project setting; new Service Connection 
-> Resource Manager; service principal (auto) 
-> scope: subscription;  resource group: rg-retail-de
-> name: retail-de-service-connection
-> Grant access permissions to all pipelines
```

Register the DevOps SP with Databricks. 
<br>
- Service connection list: manage `retail-de-service-connection`, copy appID. 
<br>
`9504eaff-a1e7-4d12-87d9-d00397197493`
- DBW user setting; Identity and Access - SP add new; Entra ID managed; paste appID. 
- name: `retail-de-service-connection`; add to DBW. 

DBW repo folder Sharing (permissions): add this SP appID, grant Can Edit. 


#### 9.2. Trigger DevOps pipeline

Note previously we have an ADF pipeline

- DevOps Pipelines create new;  code - Azure Repos Git;
- select repo;
- Configure: Existing Azure Pipelines YAML file;
- Branch: `main`;  path: `/azure-pipelines.yml`;
- Run. 

The 1st run registers the pipeline as a permanent object in DevOps.
<br>
Check DeployADF job: Service Connection's role on RG is Contributor. (Project setting, service connctions roles).


---

## 10. Power BI (not started)

Connect via the built-in Databricks connector to the workspace's SQL Warehouse — queries `retail_de.gold.*` directly, no separate data movement.

---

## Project management 
**Local device folder, ADF git, Databricks repo are each a separate checkout of the same Azure DevOps remote.**
```
cd az-retail-pipe
git add .
git commit -m "Document ADF orchestration, Azure DevOps setup, Databricks Repos repoint"
git pull azure main --no-rebase --no-edit
git push azure main
```

- ADF studio: unaffected;
- Databricks repo: Repos sidebar → the repo → Git panel → Pull

#### Databricks to local. 
If you ever edit a notebook inside Databricks Repos directly (rather than locally), that's a commit+push from within Databricks' own Git panel — separate credentials/flow again, using the PAT you configured in §8, not your local git config.


#### Sync to Git remote
```
git push -u origin main
```



## Patterns & Practice

#### 1. Azure services linked to DB: no-secret auth.
An Azure service (ADF, DevOps) needs to call Databricks...
- The service will have its Entra identity: ADF managed identity, DevOps service connection;
- Get appID from the identity;
- DBW User Identity & Acccess: Add this appID as a new Service Principal. Type: Microsoft Entra ID managed;
- Grant it permissions. 


#### 2. Pipelines in the stack
1. Databricks job `retail_de_medallion_pipeline` in DBW.
<br>
- 'How does bronze become gold?'
- Notebook tasks chained in order;
- Triggered by ADF.

2. ADF pipeline `pl_run_medallion_job` in ADF resources.
- 'When/How do the jobs start`;
- Activity `medallion_elt` triggering the DB job;
- Scheduel trigger.

3. DevOps pipeline `azure-pipelines.yml` in az-devops.
- 'How does repo code run?`
- CI/CD jobs triggered by a git push;
- Triggered by push/merge to main.
