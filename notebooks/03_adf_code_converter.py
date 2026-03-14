# Databricks notebook source
# MAGIC %md
# MAGIC # ADF → Databricks AI Code Converter
# MAGIC
# MAGIC **Purpose**: Reads the Component_Mapping and Dependency_Analysis tables produced by
# MAGIC notebooks 01 and 02, loads the raw ARM template, and calls the Databricks Foundation
# MAGIC Model API to generate production-ready Databricks code for each ADF component.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC | Source | Description |
# MAGIC |---|---|
# MAGIC | `Component_Mapping` table | From notebook 01 — component types, subtypes, Databricks equivalents, complexity scores |
# MAGIC | `Dependency_Analysis` table | From notebook 02 — dependency edges, migration phases, migration units |
# MAGIC | ARM template JSON | Raw ADF export — provides full component definitions for LLM context |
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC **`Code_Conversion`** Unity Catalog table with:
# MAGIC - Generated Databricks code (PySpark, SQL, YAML, JSON) per component
# MAGIC - Migration status tracking (success / failed / skipped)
# MAGIC - Per-unit deployment run instructions
# MAGIC - Token usage and generation metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# -- Input tables --
dbutils.widgets.text("component_mapping_table", "bircatalog.birschema.Component_Mapping", "Component Mapping Table (FQN)")
dbutils.widgets.text("dependency_analysis_table", "bircatalog.birschema.Dependency_Analysis", "Dependency Analysis Table (FQN)")
dbutils.widgets.text("arm_template_path", "/Volumes/main/default/migration/arm_template.json", "ARM Template Path (.json file or directory)")

# -- LLM --
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "Foundation Model Endpoint")

# -- Migration scope --
dbutils.widgets.text("migration_phase_filter", "all", "Migration Phase Filter (e.g. 'Phase 1' or 'all')")

# -- Target namespace --
dbutils.widgets.text("target_catalog", "bircatalog", "Target Catalog for generated code")
dbutils.widgets.text("target_schema", "birschema", "Target Schema for generated code")

# -- Output --
dbutils.widgets.text("output_catalog", "bircatalog", "Output Catalog")
dbutils.widgets.text("output_schema", "birschema", "Output Schema")
dbutils.widgets.text("output_table", "Code_Conversion", "Output Table")
dbutils.widgets.dropdown("output_write_mode", "overwrite", ["overwrite", "append"], "Write Mode")

# -- Processing --
dbutils.widgets.text("batch_size", "5", "Progress checkpoint interval")
dbutils.widgets.text("max_retries", "2", "Max LLM retries per component")

# Collect values
component_mapping_fqn   = dbutils.widgets.get("component_mapping_table")
dependency_analysis_fqn = dbutils.widgets.get("dependency_analysis_table")
arm_template_path       = dbutils.widgets.get("arm_template_path")
llm_endpoint            = dbutils.widgets.get("llm_endpoint")
migration_phase_filter  = dbutils.widgets.get("migration_phase_filter")
target_catalog          = dbutils.widgets.get("target_catalog")
target_schema           = dbutils.widgets.get("target_schema")
output_catalog          = dbutils.widgets.get("output_catalog")
output_schema           = dbutils.widgets.get("output_schema")
output_table            = dbutils.widgets.get("output_table")
output_write_mode       = dbutils.widgets.get("output_write_mode")
batch_size              = int(dbutils.widgets.get("batch_size"))
max_retries             = int(dbutils.widgets.get("max_retries"))
output_fqn              = f"{output_catalog}.{output_schema}.{output_table}"

print(f"Component Mapping: {component_mapping_fqn}")
print(f"Dependency Analysis: {dependency_analysis_fqn}")
print(f"ARM Template: {arm_template_path}")
print(f"LLM Endpoint: {llm_endpoint}")
print(f"Phase Filter: {migration_phase_filter}")
print(f"Target Namespace: {target_catalog}.{target_schema}")
print(f"Output: {output_fqn} (mode={output_write_mode})")
print(f"Batch Size: {batch_size}, Max Retries: {max_retries}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import json
import re
import time
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any

import mlflow.deployments

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

# ---- Component types that are skipped (no LLM call) ----
SKIP_TYPES = {"Parameter", "Variable", "GlobalParameter", "Credential", "ManagedVirtualNetwork", "ManagedPrivateEndpoint"}

SKIP_REASONS = {
    "Parameter": "Mapped to Workflow job parameters / dbutils.widgets.get(). Captured in Pipeline YAML.",
    "Variable": "Mapped to dbutils.jobs.taskValues / Python variables. Captured in notebook code.",
    "GlobalParameter": "Mapped to Workflow-level parameters or shared config. Captured in Pipeline YAML.",
    "Credential": "Mapped to UC Storage Credential / Secret Scope. Manual setup — provide instructions in run_instructions.",
    "ManagedVirtualNetwork": "Infrastructure config. Manual setup step — configure Databricks VNet injection.",
    "ManagedPrivateEndpoint": "Infrastructure config. Manual setup step — configure Databricks Private Link.",
}

# ---- Token budget by complexity ----
COMPLEXITY_TOKEN_MAP = {"Low": 1000, "Medium": 2000, "High": 3000}

# ---- LLM System Prompt ----
CODE_GEN_SYSTEM_PROMPT = """You are a Databricks migration engineer converting Azure Data Factory components
to production-ready Databricks code. Use current Databricks capabilities (2026).

Technology guidance:
- Lakeflow Spark Declarative Pipelines (SDP) replaces DLT:
  use `from pyspark import pipelines as dp`, `@dp.table`, `@dp.materialized_view`
- Serverless compute is GA — use it as default for jobs and notebooks
- Auto Loader: use file events mode (cloudFiles.useIncrementalListing is deprecated)
- Lakeflow Connect: GA for Salesforce, Workday, SQL Server, ServiceNow, Google Analytics
- ADF Databricks Job activity (DatabricksJob type) replaces legacy Notebook/Python/Jar activities
- Databricks Workflows support: For Each, If/Else, Run Job tasks, file arrival triggers,
  table update triggers, cron schedules
- DABs supports 22+ resource types including job, pipeline, catalog, schema, volume, secret_scope

Code rules:
1. Generate ONLY the code — no markdown fences, no explanations outside the JSON.
2. Code must be syntactically valid and runnable on Databricks.
3. Unity Catalog namespace: {target_catalog}.{target_schema}.<table_name>
4. Credentials: dbutils.secrets.get(scope, key)
5. Notebooks: error handling (try/except), logging (print), metrics (dbutils.jobs.taskValues.set)
6. YAML: DABs format with task dependencies, serverless compute
7. Follow medallion architecture (Bronze/Silver/Gold) for ingestion patterns
8. Include comments explaining ADF-to-Databricks mapping decisions

Respond with EXACTLY this JSON:
{{"code": "<generated code>", "code_language": "<python|sql|yaml|json>",
  "run_instructions": "<1-3 sentences on deploy/run steps>",
  "notes": "<migration caveats or manual steps>"}}"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def should_generate_code(component_type: str) -> bool:
    """Return True if this component type needs LLM code generation."""
    return component_type not in SKIP_TYPES


def get_skip_reason(component_type: str) -> str:
    """Return the skip reason for a component type."""
    return SKIP_REASONS.get(component_type, f"No code generation needed for {component_type}.")


def get_max_tokens(complexity: str) -> int:
    """Return the token budget for the given complexity level."""
    return COMPLEXITY_TOKEN_MAP.get(complexity, 2000)


def load_arm_templates(path: str, dbutils_ref) -> dict:
    """Load ARM template(s). Single .json file or directory of .json files."""
    if path.endswith(".json"):
        raw_text = dbutils_ref.fs.head(path)
        return json.loads(raw_text)
    else:
        # Directory mode — list and merge all .json files
        files = dbutils_ref.fs.ls(path)
        json_files = [f.path for f in files if f.path.endswith(".json")]
        if not json_files:
            print(f"WARNING: No .json files found in {path}")
            return {"resources": []}
        merged_resources = []
        merged = {}
        for fp in sorted(json_files):
            raw_text = dbutils_ref.fs.head(fp)
            parsed = json.loads(raw_text)
            merged_resources.extend(parsed.get("resources", []))
            if not merged:
                merged = parsed
        merged["resources"] = merged_resources
        print(f"Merged {len(json_files)} ARM files, {len(merged_resources)} total resources.")
        return merged


def build_arm_lookup(raw_json: dict) -> dict:
    """Build a name → resource lookup from the ARM template."""
    lookup = {}
    for r in raw_json.get("resources", []):
        raw_name = r.get("name") or r.get("Name") or ""
        # Extract the last segment from ARM-style names
        m = re.search(r"'([^']+)'\s*\)\s*\]$", raw_name)
        if m:
            path = m.group(1).lstrip("/")
            name = path.rsplit("/", 1)[-1] if "/" in path else path
        elif "/" in raw_name:
            name = raw_name.rsplit("/", 1)[-1]
        else:
            name = raw_name
        if name:
            lookup[name] = r
    return lookup


def merge_component_data(mapping_rows: list, dep_rows: list) -> list:
    """Join Component_Mapping and Dependency_Analysis by (factory_name, component_name).

    Activities, Parameters, and Variables inherit their parent pipeline's phase/unit
    data if no direct match exists.
    """
    dep_index = {}
    for d in dep_rows:
        key = (d.get("data_factory_name", ""), d.get("component_name", ""))
        dep_index[key] = d

    merged = []
    for m in mapping_rows:
        factory = m.get("data_factory_name", "")
        name = m.get("adf_component_name", "")
        dep = dep_index.get((factory, name), {})

        # If no dep data and this is a child component, inherit from parent
        if not dep and m.get("parent_component", ""):
            parent_name = m["parent_component"].split("/")[0]
            dep = dep_index.get((factory, parent_name), {})

        merged.append({
            # From Component_Mapping
            "data_factory_name": factory,
            "adf_component_type": m.get("adf_component_type", ""),
            "adf_component_name": name,
            "adf_subtype": m.get("adf_subtype", ""),
            "parent_component": m.get("parent_component", ""),
            "databricks_equivalent": m.get("databricks_equivalent", ""),
            "complexity_score": m.get("complexity_score", "Low"),
            "adf_config_summary": m.get("adf_config_summary", ""),
            "key_metrics": m.get("key_metrics", "{}"),
            # From Dependency_Analysis
            "migration_phase": dep.get("migration_phase", ""),
            "migration_unit_id": dep.get("migration_unit_id", ""),
            "migration_unit_name": dep.get("migration_unit_name", ""),
            "depends_on": dep.get("depends_on", ""),
            "depended_on_by": dep.get("depended_on_by", ""),
            "migration_unit_phase_plan": dep.get("migration_unit_phase_plan", ""),
        })
    return merged


def apply_phase_filter(components: list, phase_filter: str) -> list:
    """Filter components by migration phase. 'all' returns everything."""
    if phase_filter.strip().lower() == "all":
        return components
    target = phase_filter.strip().lower()
    return [c for c in components if c.get("migration_phase", "").strip().lower() == target]


def compute_execution_order(phase_plan_str: str) -> dict:
    """Parse a phase plan string into {component_name: order}.

    Input format: "Phase 1: LS:KV, IR:Auto | Phase 2: DS:Sales | Phase 3: Pipeline:Main"
    Returns: {"KV": 1, "Auto": 2, "Sales": 3, "Main": 4}
    """
    order_map = {}
    counter = 1
    if not phase_plan_str:
        return order_map
    for segment in phase_plan_str.split(" | "):
        # Remove the "Phase N:" prefix
        colon_pos = segment.find(":")
        if colon_pos == -1:
            continue
        entries_str = segment[colon_pos + 1:].strip()
        for entry in entries_str.split(","):
            entry = entry.strip()
            if ":" in entry:
                _, comp_name = entry.split(":", 1)
                order_map[comp_name.strip()] = counter
            else:
                order_map[entry.strip()] = counter
            counter += 1
    return order_map


def build_user_prompt(component: dict, arm_fragment: str,
                      upstream: str, downstream: str,
                      target_cat: str, target_sch: str) -> str:
    """Build the LLM user prompt as a JSON string."""
    # Truncate ARM fragment to 4000 chars
    if len(arm_fragment) > 4000:
        arm_fragment = arm_fragment[:4000] + "\n... (truncated)"
    prompt = {
        "adf_component_type": component.get("adf_component_type", ""),
        "adf_component_name": component.get("adf_component_name", ""),
        "adf_subtype": component.get("adf_subtype", ""),
        "parent_component": component.get("parent_component", ""),
        "databricks_equivalent": component.get("databricks_equivalent", ""),
        "complexity_score": component.get("complexity_score", ""),
        "adf_config_summary": component.get("adf_config_summary", ""),
        "key_metrics": component.get("key_metrics", "{}"),
        "arm_fragment": arm_fragment,
        "depends_on": upstream,
        "depended_on_by": downstream,
        "target_catalog": target_cat,
        "target_schema": target_sch,
    }
    return json.dumps(prompt, default=str)


def parse_llm_response(text: str) -> dict:
    """Extract JSON from LLM response text. Handles markdown fences and preamble."""
    text = text.strip()
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        return json.loads(text[start:end])
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError(f"Failed to parse LLM response as JSON: {e}")


def build_fallback_run_instructions(unit_components: list, order_map: dict) -> str:
    """Build deterministic numbered deployment plan for a migration unit."""
    if not unit_components:
        return ""
    sorted_comps = sorted(unit_components, key=lambda c: order_map.get(c.get("adf_component_name", ""), 999))
    lines = []
    for i, c in enumerate(sorted_comps, 1):
        ctype = c.get("adf_component_type", "")
        cname = c.get("adf_component_name", "")
        equiv = c.get("databricks_equivalent", "")
        lines.append(f"{i}. Deploy {ctype} '{cname}' as {equiv}")
    return "\n".join(lines)


def build_output_row(component: dict, llm_result: dict, timestamp: str,
                     llm_ep: str, execution_order: int) -> dict:
    """Build a full 26-column output row for a successfully processed component."""
    return {
        "conversion_timestamp": timestamp,
        "data_factory_name": component.get("data_factory_name", ""),
        "adf_component_type": component.get("adf_component_type", ""),
        "adf_component_name": component.get("adf_component_name", ""),
        "adf_subtype": component.get("adf_subtype", ""),
        "parent_component": component.get("parent_component", ""),
        "databricks_equivalent": component.get("databricks_equivalent", ""),
        "migration_phase": component.get("migration_phase", ""),
        "migration_unit_id": component.get("migration_unit_id", ""),
        "migration_unit_name": component.get("migration_unit_name", ""),
        "complexity_score": component.get("complexity_score", ""),
        "migration_status": llm_result.get("migration_status", "success"),
        "status_description": llm_result.get("status_description", ""),
        "generated_code": llm_result.get("generated_code", ""),
        "code_language": llm_result.get("code_language", "none"),
        "run_instructions": llm_result.get("run_instructions", ""),
        "execution_order": execution_order,
        "depends_on": component.get("depends_on", ""),
        "depended_on_by": component.get("depended_on_by", ""),
        "llm_endpoint_used": llm_ep,
        "llm_prompt_tokens": llm_result.get("llm_prompt_tokens", 0),
        "llm_completion_tokens": llm_result.get("llm_completion_tokens", 0),
        "generation_duration_seconds": llm_result.get("generation_duration_seconds", 0.0),
        "retry_count": llm_result.get("retry_count", 0),
        "adf_config_summary": component.get("adf_config_summary", ""),
        "key_metrics": component.get("key_metrics", "{}"),
    }


def build_skipped_row(component: dict, timestamp: str, llm_ep: str,
                      execution_order: int) -> dict:
    """Build a 26-column output row for a skipped component."""
    ctype = component.get("adf_component_type", "")
    return {
        "conversion_timestamp": timestamp,
        "data_factory_name": component.get("data_factory_name", ""),
        "adf_component_type": ctype,
        "adf_component_name": component.get("adf_component_name", ""),
        "adf_subtype": component.get("adf_subtype", ""),
        "parent_component": component.get("parent_component", ""),
        "databricks_equivalent": component.get("databricks_equivalent", ""),
        "migration_phase": component.get("migration_phase", ""),
        "migration_unit_id": component.get("migration_unit_id", ""),
        "migration_unit_name": component.get("migration_unit_name", ""),
        "complexity_score": component.get("complexity_score", ""),
        "migration_status": "skipped",
        "status_description": get_skip_reason(ctype),
        "generated_code": "",
        "code_language": "none",
        "run_instructions": "",
        "execution_order": execution_order,
        "depends_on": component.get("depends_on", ""),
        "depended_on_by": component.get("depended_on_by", ""),
        "llm_endpoint_used": llm_ep,
        "llm_prompt_tokens": 0,
        "llm_completion_tokens": 0,
        "generation_duration_seconds": 0.0,
        "retry_count": 0,
        "adf_config_summary": component.get("adf_config_summary", ""),
        "key_metrics": component.get("key_metrics", "{}"),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Input Tables

# COMMAND ----------

# Load Component_Mapping
mapping_df = spark.table(component_mapping_fqn)
mapping_rows = [row.asDict() for row in mapping_df.collect()]
print(f"Loaded {len(mapping_rows)} rows from {component_mapping_fqn}")

# Load Dependency_Analysis
dep_df = spark.table(dependency_analysis_fqn)
dep_rows = [row.asDict() for row in dep_df.collect()]
print(f"Loaded {len(dep_rows)} rows from {dependency_analysis_fqn}")

# Load ARM template(s)
try:
    arm_json = load_arm_templates(arm_template_path, dbutils)
    arm_lookup = build_arm_lookup(arm_json)
    print(f"Loaded ARM template: {len(arm_lookup)} components in lookup")
except Exception as e:
    print(f"WARNING: Could not load ARM template from {arm_template_path}: {e}")
    print("Proceeding without ARM fragments — LLM will use config_summary and key_metrics only.")
    arm_json = {"resources": []}
    arm_lookup = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge, Filter & Order

# COMMAND ----------

# Merge Component_Mapping + Dependency_Analysis
merged_components = merge_component_data(mapping_rows, dep_rows)
print(f"Merged: {len(merged_components)} components")

# Apply phase filter
filtered_components = apply_phase_filter(merged_components, migration_phase_filter)
print(f"After phase filter ('{migration_phase_filter}'): {len(filtered_components)} components")

# Compute execution order per migration unit
unit_order_maps = {}
for c in filtered_components:
    uid = c.get("migration_unit_id", "")
    if uid and uid not in unit_order_maps:
        plan = c.get("migration_unit_phase_plan", "")
        unit_order_maps[uid] = compute_execution_order(plan)

# Sort by migration_unit_id then execution_order
def _sort_key(c):
    uid = c.get("migration_unit_id", "")
    name = c.get("adf_component_name", "")
    order = unit_order_maps.get(uid, {}).get(name, 999)
    return (uid, order, name)

filtered_components.sort(key=_sort_key)
print(f"Sorted {len(filtered_components)} components by migration unit and execution order.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM Code Generation

# COMMAND ----------

client = mlflow.deployments.get_deploy_client("databricks")

def generate_code(component: dict, arm_fragment: str,
                  upstream: str, downstream: str) -> dict:
    """Call the LLM to generate Databricks code for a component.

    Returns dict with: migration_status, status_description, generated_code,
    code_language, run_instructions, generation_duration_seconds, retry_count,
    llm_prompt_tokens, llm_completion_tokens.
    """
    system_prompt = CODE_GEN_SYSTEM_PROMPT.format(
        target_catalog=target_catalog,
        target_schema=target_schema,
    )
    user_prompt = build_user_prompt(
        component, arm_fragment, upstream, downstream,
        target_catalog, target_schema,
    )
    max_tok = get_max_tokens(component.get("complexity_score", "Medium"))

    last_error = ""
    for attempt in range(max_retries + 1):
        start_time = time.time()
        try:
            resp = client.predict(
                endpoint=llm_endpoint,
                inputs={
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "max_tokens": max_tok,
                    "temperature": 0.05,
                },
            )
            duration = time.time() - start_time
            text = resp["choices"][0]["message"]["content"].strip()
            usage = resp.get("usage", {})
            parsed = parse_llm_response(text)

            return {
                "migration_status": "success",
                "status_description": parsed.get("notes", ""),
                "generated_code": parsed.get("code", ""),
                "code_language": parsed.get("code_language", "python"),
                "run_instructions": parsed.get("run_instructions", ""),
                "generation_duration_seconds": round(duration, 2),
                "retry_count": attempt,
                "llm_prompt_tokens": usage.get("prompt_tokens", 0),
                "llm_completion_tokens": usage.get("completion_tokens", 0),
            }
        except Exception as e:
            duration = time.time() - start_time
            last_error = str(e)
            if attempt < max_retries:
                wait = 2 ** attempt
                print(f"  Retry {attempt + 1}/{max_retries} after {wait}s: {e}")
                time.sleep(wait)

    return {
        "migration_status": "failed",
        "status_description": f"LLM generation failed after {max_retries + 1} attempts: {last_error}",
        "generated_code": "",
        "code_language": "none",
        "run_instructions": "",
        "generation_duration_seconds": round(duration, 2),
        "retry_count": max_retries,
        "llm_prompt_tokens": 0,
        "llm_completion_tokens": 0,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Loop

# COMMAND ----------

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
rows = []

for i, component in enumerate(filtered_components):
    ctype = component.get("adf_component_type", "")
    cname = component.get("adf_component_name", "")
    uid = component.get("migration_unit_id", "")
    order = unit_order_maps.get(uid, {}).get(cname, 999)

    if not should_generate_code(ctype):
        # Skip — no LLM call
        row = build_skipped_row(component, timestamp, llm_endpoint, order)
        rows.append(row)
        print(f"[{i+1}/{len(filtered_components)}] SKIP {ctype}/{cname}")
        continue

    # Get ARM fragment for LLM context
    arm_fragment = ""
    arm_resource = arm_lookup.get(cname, {})
    if arm_resource:
        arm_fragment = json.dumps(arm_resource, indent=2, default=str)

    upstream = component.get("depends_on", "")
    downstream = component.get("depended_on_by", "")

    print(f"[{i+1}/{len(filtered_components)}] GENERATE {ctype}/{cname} ...", end=" ")
    llm_result = generate_code(component, arm_fragment, upstream, downstream)
    print(f"→ {llm_result['migration_status']}")

    row = build_output_row(component, llm_result, timestamp, llm_endpoint, order)
    rows.append(row)

    # Progress checkpoint
    if (i + 1) % batch_size == 0:
        print(f"  Progress: {i+1}/{len(filtered_components)} components processed.")

print(f"\nProcessing complete: {len(rows)} rows generated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-Unit Run Instructions

# COMMAND ----------

# Group rows by migration_unit_id for holistic run instructions
unit_groups = defaultdict(list)
for row in rows:
    uid = row.get("migration_unit_id", "")
    if uid:
        unit_groups[uid].append(row)

for uid, unit_rows in unit_groups.items():
    # Build a holistic deployment plan via LLM
    unit_name = unit_rows[0].get("migration_unit_name", uid)
    components_summary = []
    for r in sorted(unit_rows, key=lambda x: x.get("execution_order", 999)):
        components_summary.append({
            "name": r["adf_component_name"],
            "type": r["adf_component_type"],
            "status": r["migration_status"],
            "databricks_equivalent": r["databricks_equivalent"],
            "code_language": r["code_language"],
        })

    order_map = unit_order_maps.get(uid, {})

    try:
        plan_prompt = json.dumps({
            "migration_unit_id": uid,
            "migration_unit_name": unit_name,
            "components": components_summary,
        }, default=str)

        resp = client.predict(
            endpoint=llm_endpoint,
            inputs={
                "messages": [
                    {"role": "system", "content": (
                        "You are a Databricks deployment architect. Given a migration unit's "
                        "components, generate a numbered deployment plan. Be specific about "
                        "the order: deploy infrastructure first (secrets, connections), then "
                        "DDL (tables, schemas), then notebooks, then workflows, then triggers. "
                        "Respond with ONLY the numbered list, no JSON wrapper."
                    )},
                    {"role": "user", "content": plan_prompt},
                ],
                "max_tokens": 500,
                "temperature": 0.05,
            },
        )
        plan_text = resp["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"  Run instructions fallback for {uid}: {e}")
        plan_text = build_fallback_run_instructions(unit_rows, order_map)

    # Update all rows in this unit with the shared plan
    for r in unit_rows:
        r["run_instructions"] = plan_text

print(f"Updated run instructions for {len(unit_groups)} migration units.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Unity Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {output_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_catalog}.{output_schema}")

results_df = spark.createDataFrame(rows)
results_df.write.format("delta").mode(output_write_mode).saveAsTable(output_fqn)

written = spark.table(output_fqn).count()
print(f"Wrote {len(rows)} rows to {output_fqn} (mode={output_write_mode})")
print(f"Table now has {written} total rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

from collections import Counter

status_counter = Counter(r["migration_status"] for r in rows)
phase_counter = Counter(r["migration_phase"] for r in rows)
lang_counter = Counter(r["code_language"] for r in rows if r["migration_status"] == "success")

total_prompt_tokens = sum(r.get("llm_prompt_tokens", 0) for r in rows)
total_completion_tokens = sum(r.get("llm_completion_tokens", 0) for r in rows)
gen_rows = [r for r in rows if r["migration_status"] == "success" and r["generation_duration_seconds"] > 0]
avg_gen_time = sum(r["generation_duration_seconds"] for r in gen_rows) / len(gen_rows) if gen_rows else 0.0

print("=" * 75)
print(f"CODE CONVERSION SUMMARY")
print("=" * 75)
print(f"Total components processed: {len(rows)}")
print()
print("Migration Status:")
for status in ["success", "failed", "skipped"]:
    print(f"  {status:10s}: {status_counter.get(status, 0)}")
print()
print("By Migration Phase:")
for phase in sorted(phase_counter):
    if phase:
        success = sum(1 for r in rows if r["migration_phase"] == phase and r["migration_status"] == "success")
        failed = sum(1 for r in rows if r["migration_phase"] == phase and r["migration_status"] == "failed")
        skipped = sum(1 for r in rows if r["migration_phase"] == phase and r["migration_status"] == "skipped")
        print(f"  {phase}: {phase_counter[phase]} total (success={success}, failed={failed}, skipped={skipped})")
print()
print("Code Languages (successful):")
for lang, cnt in lang_counter.most_common():
    print(f"  {lang:8s}: {cnt}")
print()
print("Token Usage:")
print(f"  Prompt tokens:     {total_prompt_tokens:,}")
print(f"  Completion tokens: {total_completion_tokens:,}")
print(f"  Total tokens:      {total_prompt_tokens + total_completion_tokens:,}")
print()
print(f"Avg generation time: {avg_gen_time:.1f}s per component")
print()
print(f"Results written to: {output_fqn}")
