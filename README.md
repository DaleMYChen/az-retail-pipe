### Project outline & components
- Azure Data Factory for ingestion (= Airflow);
- Databricks for ELT;
- Power BI for analytics. 



# retail-azure-de-project

Faker-generated retail data, ingested and transformed on Azure, mirroring the *intent* of the
BigQuery/dbt/Airflow version — but built the way Azure DE actually works, not forced into the old shape.

Flow: **ADF (ingest) → Databricks (transform, medallion) → Power BI (report)**, with
**Azure DevOps (Repos + one YAML pipeline)** wrapping the whole thing in CI/CD.

---

## 0. A map of the environment first

Before touching any tool, it's worth knowing where everything lives, because on Azure your project
is spread across **two separate ecosystems** that don't share a UI:

**Inside one Azure Resource Group** (`rg-retail-de`, your billing/permissions boundary):
- **Storage Account** → contains ADLS Gen2, which is just blob storage with a hierarchical namespace
  turned on. Inside it, **containers** (think top-level folders) hold your data: `raw`, `bronze`,
  `silver`, `gold`.
- **Data Factory** instance → holds *pipelines* (orchestration definitions), not data itself.
- **Databricks workspace** → an Azure-managed resource, but once created it opens its **own web UI**
  outside the Azure Portal. It holds *notebooks*, *clusters/jobs*, and a *metastore* (Unity Catalog or
  legacy Hive metastore) which is the catalog of table names that point at Delta files sitting in ADLS.

**Outside the resource group, separate SaaS products:**
- **Power BI** — its own service (powerbi.com), connects *into* your Databricks/Azure resources as a
  data source.
- **Azure DevOps** — an entirely separate organization-level product (dev.azure.com), holding your
  **Repos** (git) and **Pipelines** (CI/CD). It talks to Azure resources via a **Service Connection**
  (a service principal with scoped permissions) — it's not "inside" your subscription, it *acts on* it.

**How an object moves across all of this, end to end:**

```
Faker CSV (your laptop)
   │  ADF Copy Activity
   ▼
ADLS Gen2 /raw            (files: customers.csv, orders.csv, ...)
   │  ADF triggers a Databricks Job (chained notebook tasks)
   ▼
Databricks "bronze" notebook  → writes Delta files to ADLS /bronze, registers table bronze.orders
   │
Databricks "silver" notebook  → reads table bronze.orders, writes /silver, registers silver.orders_validated
   │
Databricks "gold" notebook    → reads silver tables, writes /gold, registers gold.fct_orders
   ▼
Power BI  → connects via Databricks SQL Warehouse (JDBC/ODBC) → queries gold.fct_orders directly
```

Three flows in Azure DE:
1. Data flow. 
<br>
CSV `→` ADLS `/raw` `→` Delta files in `/bronze /silver /gold` `→` Power BI query

2. Orchestration flow (= Airflow). 
<br>
ADF's pipeline trigger fires `→` ADF calls the Databricks Job `→` the Job runs its three tasks orchestrated by Databricks Workflows `→` ADF gets a success/fail signal back.

Orchestration layer 1: ADF pipeline triggering Job; 
<br>
Orchestration layer 2: tasks chained by DB Workflows in the Job.

3. CI/CD flow. 
The git repo: `Azure DevOps Repos = {ADF pipeline JSON and Databricks notebook source}`, deployed by YAML into the actual ADF instance and Databricks workspace.

> **Your question — "are bronze/gold notebooks or repos?"**
> They're **notebooks** (or `.py` files if you prefer files-in-Repos over the notebook UI) — one
> notebook per layer is the common, mature pattern: `01_bronze_ingest.py`, `02_silver_transform.py`,
> `03_gold_aggregate.py`. Each notebook *reads and writes Delta tables*, not files you pass around
> manually. "Repos" is just the git-sync mechanism that puts these notebook files under version
> control and connects the Databricks workspace to your Azure DevOps repo.
> The three notebooks are then chained together as
> **tasks in one Databricks Job** (multi-task workflow), and *that job* is what ADF calls.

A deliberate departure from your BigQuery project: there, dbt's `stg/int/marts` layers were **SQL
models compiled by one tool**. On Azure, the mature/common pattern is **PySpark notebooks writing
Delta tables directly**, with Databricks Workflows doing the chaining (= DAG). 



---

## 1. Setup
**Azure (free tier, already have an account)**

In Azure CloudShell, set account and create resource group, storage account
- Resource group `rg-retail-de`
```
az account list --output table
az account set --subscription "<subscription-id-or-name>"

az group create --name rg-retail-de --location australiaeast

az storage account create \
  --name retaildesadc \
  --resource-group rg-retail-de \
  --location australiaeast \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true
```

- Storage account with ADLS Gen2 (hierarchical namespace) enabled; create containers `raw`, `bronze`, `silver`, `gold`

```
for c in raw bronze silver gold; do
  az storage container create \
    --account-name retaildesadc \
    --name $c \
    --auth-mode login
done
```


- Data Factory instance (pipelines to be set up later in ADF studio)
<br>
`az datafactory create --resource-group rg-retail-de --factory-name adf-retail-de-dc`

- Databricks workspace (Standard is retired)

'''
az databricks workspace create \
  --resource-group rg-retail-de \
  --name dbw-retail-de \
  --location australiaeast \
  --sku premium
'''

Verify creation:
```
az resource list --resource-group rg-retail-de --output table
```

**Azure DevOps**
- One Azure DevOps project, one Repos git repo (this becomes the source of truth both ADF and
  Databricks Repos sync from)
- A **Service Connection** from Azure DevOps → your Azure subscription (service principal, scoped to
  `rg-retail-de`) — this is what lets the YAML pipeline deploy without you typing credentials

---

## 2. Data generation (unchanged from your original project)

`data_generator/generate_raw_data.py` — same Faker logic, same noise injections (nulls, inconsistent
datetime formats, settlement-before-payment, negative values, orphaned FKs). Output stays as CSV.

```
pip install -r requirements.txt
python data_generator/generate_raw_data.py --out-dir data
```

CloudShell upload the 7 CSVs. Push to `/raw` container:
```
az storage blob upload-batch \
  --account-name retaildesadc \
  --destination raw \
  --source ~ \
  --pattern "*.csv" \
  --auth-mode login
```


---

## 3. Unity Catalog setup. 

3.1. Create an Access Connector for Azure Databricks (a managed identity Databricks UC uses to reach your storage).
```
az databricks access-connector create \
  --resource-group rg-retail-de \
  --name ac-retail-de \
  --location australiaeast \
  --identity-type SystemAssigned
```

3.2. Grant the connector write access to SA.
```
CONNECTOR_ID=$(az databricks access-connector show \
  --resource-group rg-retail-de --name ac-retail-de --query id -o tsv)
PRINCIPAL_ID=$(az databricks access-connector show \
  --resource-group rg-retail-de --name ac-retail-de --query identity.principalId -o tsv)

az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee-object-id "$PRINCIPAL_ID" \
  --assignee-principal-type ServicePrincipal \
  --scope "/subscriptions/$(az account show --query id -o tsv)/resourceGroups/rg-retail-de/providers/Microsoft.Storage/storageAccounts/retaildesadc"
```

**3.3. Create the external location for UC**:
Azure portal: resource group -> dbw-retail-de DB instance -> Launch workspace. 

Catalog `→` Create credential `
```
name: cred-retail-de

access connector id: 
/subscriptions/df2f768b-28b8-42ff-be15-7896bd8b9004/resourceGroups/rg-retail-de/providers/Microsoft.Databricks/accessConnectors/ac-retail-de
```
Obtain access connector id: (CloudShell) 
```
az databricks access-connector show \
  --resource-group rg-retail-de --name ac-retail-de --query id -o tsv
```

Create a physical container
<br>
`az storage container create --account-name retaildesadc --name catalog-root --auth-mode login` 


(Force) Create an external location for UC (Databricks workspace - Catalog Create):
```
name: ext-catalog-root
URL: abfss://catalog-root@retaildesadc.dfs.core.windows.net/
storage credential: choose the newly created cred-retail-de
```



3.5. Create catalog + schemas.
<br>
Databricks workspace - Catalog

```
name: retail_de
storage location: retail_de
```

3.6. Schema creation for UC. 

1. Force create external locations for bronze, silver and gold using `cred-retail-de` credentials. 
```
ext-raw   abfss://raw@retaildesadc.dfs.core.windows.net/
ext-bronze	abfss://bronze@retaildesadc.dfs.core.windows.net/
ext-silver	abfss://silver@retaildesadc.dfs.core.windows.net/
ext-gold	abfss://gold@retaildesadc.dfs.core.windows.net/
```

2. Create the 3 schemas in SQL editor. 
```
CREATE SCHEMA IF NOT EXISTS retail_de.bronze
MANAGED LOCATION 'abfss://bronze@retaildesadc.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS retail_de.silver
MANAGED LOCATION 'abfss://silver@retaildesadc.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS retail_de.gold
MANAGED LOCATION 'abfss://gold@retaildesadc.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS retail_de.raw;
CREATE EXTERNAL VOLUME retail_de.raw.landing
LOCATION 'abfss://raw@retaildesadc.dfs.core.windows.net/';
```

Go back to Catalog explorer to confirm 5 schemas created `default, information_schema, raw, bronze, silver, gold`.

Why external volumn for raw landing?
<br>
DB notebook reads files at `/Volumes/retail_de/raw/landing/customers.csv` instead of a raw `abfss://` path

---

## 4. Transformation — Databricks (PySpark + Delta Lake)

Three notebooks, chained as a **Databricks Workflow (Job)** with task dependencies (`bronze → silver
→ gold`), each notebook reading the previous layer's Delta table and writing its own:

#### 4.1. Bronze (staging)

**`01_bronze_ingest.py`**
- Read raw CSVs from ADLS `/raw`
- Cast types, add `_ingested_at`, flag (don't drop) bad rows — equivalent to your staging `is_null`
  columns
- Write Delta to `/bronze`, register as table (e.g. `bronze.orders`)

#### Action:
<br>
Workspace sidebar → your user folder → Import → upload .py
<br>
Attach to serverless compite, Run All. 

---

### 4.2. Silver (Intermediate + SCD2)

**SCD2 implementation: dbt vs DB**
<br>
dbt's snapshot block ran as a scheduled job that compared the live source table against the last snapshot, using `updated_at` to decide what changed.

Databricks (current-silver vs snapshot-bronze): there's no live source to keep re-checking — bronze is a batch snapshot itself, refreshed each run. So the silver SCD2 table compares this run's bronze row against the current open row (`dbt_valid_to` IS NULL equivalent) already sitting in the silver Delta table, and:

- if nothing about the tracked columns changed → do nothing
- if something changed → close the old row (valid_to = now) and insert a new one (valid_from = now, valid_to = null)


**02a_silver_scd2.py** — customers/products, using a Delta MERGE to detect changes and version them.

---
#### Non-SCD2 silver tables.

**02b_silver_transform.py** — the four validation/enrichment tables: order_items_validated, order_items_priced, orders_enriched, payments_validated, returns_matched.

--- 

#### 4.3. Gold

**`03_gold_aggregate.py`**
- `fct_order_items`, `fct_orders` — same aggregation intent as your marts layer, written as Delta
  tables Power BI will query directly

**Data quality tests**: instead of dbt's schema tests, this project uses lightweight PySpark
assertions per notebook (row counts, null checks, a singular test like "no `fct_orders` row has
`order_net_amount < 0`") — same intent as your `/tests`, just not a separate framework unless you
later want Great Expectations.

---

## 5. Databricks Workflow chaining. 

Set up Databricks Job which chains the 4 notebooks as a workflow. 
- Databricks Workspace: Jobs & Pipelines create job;
<br>
name: `retail_de_medallion_pipeline`.
- Task 1. `bronze_ingest`. 
<br>
Type: Notebook, Source: the actual notebook, Serverless Compute, No dependency (1st task). 
- Task 2. `silver_scd2`. Depends on `bronze_ingest`. 
- Task 3. `silver_transform`. Depends on `silver_scd2`. 
- Task 4. `gold_aggregate`. Dependes on `silver_transform`. 

Run Now to test the chain. 
<br>
`Job id: 1035721459403107`

---

## 6. ADF Orchestration.

#### 6.1. Authenticate ADF & grant permissions

Confirm ADF has a system-assigned identity (principalId):
```
az datafactory show --resource-group rg-retail-de --factory-name adf-retail-de-dc --query identity
```

Obtain Application ID (to register a new service principal):
```
az ad sp show --id 2f449f4e-845d-4536-a960-d5dece2dc52f --query appId -o tsv
```
`871c1918-db93-4566-a97e-cb800bad235f`

Register service principal with the above ID (User Setting; Identity and access -> User management -> Service prinicpals -> Add)
<br>
Select MS Entra ID managed;
<br>
`name: adf-retail-de-dc`. 
<br>
Add the service principal the Databricks workspace access `dbw-retail-de`. 


Databricks workspace, Jobs & Pipelines -> job permissions -> Add principal. 
<br>
Search for ADF identity `adf-retail-de-dc`. Grant `Can Manage Run`. Add.


### 6.2. Linked service in ADF Studio. 

`adf.azure.com` Select the existing ADF instance. 

Manage; Linked services -> New; Azure Databricks. 
```
AutoResolveIntegrationRuntime
Databricks workspace: dbw-retail-de
Authentication type: managed service identity
Cluster: pick any placeholder option
```

### 6.3. Create new pipeline in ADF Studio. 
```
name: pl_run_medallion_job
activities -> Databricks Job -> drag to canvas
Settings; Databricks linked service -> dbw-retail-de -> Job; select retail_de_medallion_pipeline
Debug
```
---

## 7. Azure DevOps CI/CD

#### 7.1. Set up for Azure DevOps
1. Azure portal -> Azure DevOps organisations -> Create named `retail-de-lab`. 
2. Create new projecy in this org `retail-azure-de-project`. 
3. Create personal access token (dev.azure Org -> User setting)
```
Am2Ia7M4oblEAXQvEl5cHj6URz5Y0FaIz5kXwP0bZcVVtnNPPhrHJQQJ99CIACAAAAAAAAAAAAASAZDO2YYR
```

4. Push local repo to Azure DevOps 


```
git remote add azure https://dev.azure.com/retail-de-lab/az-retail-pipe/_git/az-retail-pipe
git push azure --all
```

---

## 8. Reporting — Power BI

Power BI connects to the Databricks **SQL Warehouse** (a lightweight SQL endpoint over your Delta
gold tables) via the built-in Databricks connector — no separate data movement step, it queries the
gold Delta tables where they already sit.

---

## 6. Azure DevOps — Repos + one YAML pipeline

Kept deliberately simple, per your ask — one pipeline, two jobs, triggered on merge to `main`:

```yaml
# azure-pipelines.yml (sketch — filled in properly during implementation)
trigger:
  branches:
    include: [main]

jobs:
  - job: DeployADF
    steps:
      - task: AzureCLI@2   # publishes ADF's ARM template (from adf_publish branch) to the workspace
        inputs:
          azureSubscription: 'retail-de-service-connection'
          scriptType: bash
          scriptLocation: inlineScript
          inlineScript: |
            az datafactory ... # deploy ARM template

  - job: SyncDatabricksRepo
    steps:
      - task: AzureCLI@2   # tells the Databricks workspace's prod Repo to pull latest main
        inputs:
          azureSubscription: 'retail-de-service-connection'
          scriptType: bash
          scriptLocation: inlineScript
          inlineScript: |
            databricks repos update --path /Repos/prod/retail-azure-de-project --branch main
```

**What this buys you over your old docker-compose trigger:** merging to `main` is the only manual
step — ADF's ARM template and the Databricks workspace's production notebooks both update themselves.
No SSH, no `docker compose up`, no manually-copied service account key file.

---

## 7. Suggested build order

1. Provision resources (CLI/portal), confirm `az login` + Databricks workspace reachable from VS Code
2. Faker generator → manual upload to `/raw` (prove the data's right before automating)
3. Bronze notebook, run manually in Databricks UI
4. Silver notebook (incl. SCD2 MERGE), run manually
5. Gold notebook, run manually
6. Chain all three into one Databricks Job
7. Build the ADF pipeline (Copy + trigger the Job), run it end to end manually
8. Connect Power BI, build 2–3 visuals off gold tables
9. Set up Azure DevOps Repos, connect Databricks Repos to it, write the YAML pipeline
10. Add a trigger to ADF so the whole thing runs on a schedule, not just manually

---

*Next: work through step 1 together — provisioning the resource group, storage account, ADF, and
Databricks workspace, with the "why" noted for each non-obvious choice (e.g. Standard vs Premium
Databricks tier, why ADLS Gen2 needs hierarchical namespace on).*











