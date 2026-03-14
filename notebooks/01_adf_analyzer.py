# Databricks notebook source
# MAGIC %md
# MAGIC # ADF Dependency Analyzer
# MAGIC
# MAGIC **Purpose**: Parses an ADF ARM template (full factory export or single pipeline extract),
# MAGIC builds a complete dependency graph across all component types, classifies components into
# MAGIC migration phases using topological layering + LLM refinement, and generates visual
# MAGIC dependency diagrams.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Supported Input Formats
# MAGIC
# MAGIC ### Option A — Full ADF ARM Template
# MAGIC
# MAGIC Standard export via **ADF Studio → Manage → ARM Template → Export ARM Template**.
# MAGIC ```json
# MAGIC {
# MAGIC   "$schema": "http://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
# MAGIC   "contentVersion": "1.0.0.0",
# MAGIC   "parameters": { "factoryName": { "type": "string" } },
# MAGIC   "resources": [
# MAGIC     { "type": "Microsoft.DataFactory/factories", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/pipelines", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/datasets", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/linkedservices", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/triggers", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/dataflows", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/integrationruntimes", ... }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Option B — Single Pipeline Extract
# MAGIC
# MAGIC JSON from `Get-AzDataFactoryV2Pipeline | ConvertTo-Json -Depth 100`.
# MAGIC Accepts a single object or an array. Both `name`/`Name` and
# MAGIC `properties`/`Properties` casings are handled.
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "PipelineName",
# MAGIC   "properties": {
# MAGIC     "activities": [ ... ],
# MAGIC     "parameters": { ... },
# MAGIC     "variables": { ... }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC - **Unity Catalog table** (parameterized) with dependency edges, phases, migration units, and LLM reasoning
# MAGIC - **Dependency graph images** rendered with NetworkX + matplotlib (displayed inline)
# MAGIC
# MAGIC ### Enrichment Columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `inferred_domain` | Business domain inferred from naming patterns (e.g. "sales", "finance", "supply_chain") |
# MAGIC | `activity_types` | Comma-separated activity types for pipelines (e.g. "Copy, ExecuteDataFlow, ForEach") |
# MAGIC
# MAGIC ### Migration Unit Columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `migration_unit` | Classification: Single Pipeline, Pipeline Group, Application, or Domain |
# MAGIC | `migration_unit_name` | Descriptive name for the migration unit (e.g. "Sales Data ETL") |
# MAGIC | `migration_unit_id` | Stable ID for grouping — all components in the same unit share the same ID |
# MAGIC | `migration_unit_members` | All components in this unit, formatted as `Type:Name` (comma-separated) |
# MAGIC | `migration_unit_phase_plan` | Per-phase migration roadmap within the unit (e.g. `Phase 1: LS:KeyVault \| Phase 2: DS:Sales`) |
# MAGIC | `migration_unit_reason` | LLM-generated justification for the classification |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - `networkx`, `matplotlib` (pre-installed on DBR 14+)
# MAGIC - A Databricks Foundation Model API serving endpoint for phase-reasoning

# COMMAND ----------

# MAGIC %pip install networkx matplotlib --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# -- Input parameters --
dbutils.widgets.dropdown("input_mode", "arm_template", ["arm_template", "single_pipeline"], "Input Mode")
dbutils.widgets.text("input_path", "/Volumes/main/default/migration/arm_template.json", "Path to JSON (DBFS / Volumes / workspace)")
dbutils.widgets.text("factory_name", "", "Data Factory Name (auto-detected if blank)")

# -- LLM parameters --
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "Foundation Model Endpoint")

# -- Output parameters --
dbutils.widgets.text("output_catalog", "bircatalog", "Output Unity Catalog")
dbutils.widgets.text("output_schema", "birschema", "Output Schema")
dbutils.widgets.text("output_table", "Dependency_Analysis", "Output Table Name")
dbutils.widgets.dropdown("output_write_mode", "overwrite", ["overwrite", "append"], "Table Write Mode")

# -- Graph parameters --
dbutils.widgets.dropdown("render_graph", "yes", ["yes", "no"], "Render dependency graph images?")

# Collect values
input_mode          = dbutils.widgets.get("input_mode")
input_path          = dbutils.widgets.get("input_path")
factory_name_param  = dbutils.widgets.get("factory_name").strip()
llm_endpoint        = dbutils.widgets.get("llm_endpoint")
output_catalog      = dbutils.widgets.get("output_catalog")
output_schema       = dbutils.widgets.get("output_schema")
output_table        = dbutils.widgets.get("output_table")
output_write_mode   = dbutils.widgets.get("output_write_mode")
render_graph        = dbutils.widgets.get("render_graph") == "yes"
output_fqn          = f"{output_catalog}.{output_schema}.{output_table}"

print(f"Input mode:    {input_mode}")
print(f"Input path:    {input_path}")
print(f"LLM endpoint:  {llm_endpoint}")
print(f"Output table:  {output_fqn}  (mode={output_write_mode})")
print(f"Render graph:  {render_graph}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import json
import re
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any

import networkx as nx
import matplotlib
matplotlib.use("Agg")           # non-interactive backend for Databricks
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

import mlflow.deployments

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load & Parse Input

# COMMAND ----------

raw_text = dbutils.fs.head(input_path)
raw_json = json.loads(raw_text)

# ---- ARM resource-type → friendly label ----
TYPE_MAP = {
    "Microsoft.DataFactory/factories/pipelines":          "Pipeline",
    "Microsoft.DataFactory/factories/datasets":           "Dataset",
    "Microsoft.DataFactory/factories/linkedservices":     "LinkedService",
    "Microsoft.DataFactory/factories/triggers":           "Trigger",
    "Microsoft.DataFactory/factories/dataflows":          "DataFlow",
    "Microsoft.DataFactory/factories/integrationruntimes":"IntegrationRuntime",
    "Microsoft.DataFactory/factories/adfcdcs":            "ChangeDataCapture",
    "Microsoft.DataFactory/factories/credentials":        "Credential",
    "Microsoft.DataFactory/factories/globalParameters":   "GlobalParameter",
    "Microsoft.DataFactory/factories/managedVirtualNetworks":                         "ManagedVirtualNetwork",
    "Microsoft.DataFactory/factories/managedVirtualNetworks/managedPrivateEndpoints": "ManagedPrivateEndpoint",
    "Microsoft.DataFactory/factories/privateEndpointConnections":                     "PrivateEndpointConnection",
}

def _extract_name(resource: dict) -> str:
    raw = resource.get("name") or resource.get("Name") or "unknown"
    m = re.search(r"'([^']+)'\s*\)\s*\]$", raw)
    if m:
        path = m.group(1).lstrip("/")
        return path.rsplit("/", 1)[-1] if "/" in path else path
    if "/" in raw:
        return raw.rsplit("/", 1)[-1]
    return raw

def _props(resource: dict) -> dict:
    return resource.get("properties") or resource.get("Properties") or {}

def parse_arm_template(template: dict):
    resources = template.get("resources", [])
    factory = ""
    components = []
    for r in resources:
        rtype = r.get("type", "")
        friendly = TYPE_MAP.get(rtype)
        if rtype == "Microsoft.DataFactory/factories":
            factory = _extract_name(r) or "UnknownFactory"
            continue
        if friendly:
            components.append({"type": friendly, "name": _extract_name(r), "properties": _props(r)})
    return factory, components

def parse_single_pipelines(data):
    if isinstance(data, dict):
        data = [data]
    comps = []
    for p in data:
        name  = p.get("name") or p.get("Name") or "unknown"
        props = p.get("properties") or p.get("Properties") or {}
        comps.append({"type": "Pipeline", "name": name, "properties": props})
    return "", comps

if input_mode == "arm_template":
    detected_factory, components = parse_arm_template(raw_json)
else:
    detected_factory, components = parse_single_pipelines(raw_json)

factory_name = factory_name_param or detected_factory or "UnknownFactory"

# Build a quick lookup
comp_lookup = {c["name"]: c for c in components}

print(f"Data Factory: {factory_name}")
print(f"Components:   {len(components)}")
for ctype in sorted({c["type"] for c in components}):
    print(f"  {ctype}: {sum(1 for c in components if c['type'] == ctype)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract All Dependency Edges
# MAGIC
# MAGIC Captures **cross-component** relationships:
# MAGIC
# MAGIC | From | To | Relationship |
# MAGIC |---|---|---|
# MAGIC | Pipeline | Pipeline | `ExecutePipeline` activity |
# MAGIC | Pipeline | DataFlow | `ExecuteDataFlow` activity |
# MAGIC | Pipeline | Dataset | Activity `inputs` / `outputs` |
# MAGIC | Pipeline | LinkedService | Activity staging / linked-service refs |
# MAGIC | Pipeline | IntegrationRuntime | Activity `integrationRuntime` ref |
# MAGIC | DataFlow | Dataset | `sources` / `sinks` dataset refs |
# MAGIC | Dataset | LinkedService | `linkedServiceName` |
# MAGIC | LinkedService | LinkedService | Key Vault or nested service refs |
# MAGIC | Trigger | Pipeline | `pipelines[].pipelineReference` |

# COMMAND ----------

edges = []          # list of (from_name, to_name, edge_type)
component_names = {c["name"] for c in components}

def _add(src, tgt, rel):
    """Add an edge only if the target exists in the factory."""
    if tgt and tgt in component_names:
        edges.append((src, tgt, rel))

def _scan_for_refs(obj, src_name, depth=0):
    """Recursively scan a dict/list for referenceName pointers."""
    if depth > 15:
        return
    if isinstance(obj, dict):
        ref = obj.get("referenceName")
        ref_type = obj.get("type", "")
        if ref and ref in component_names:
            if "LinkedServiceReference" in ref_type:
                _add(src_name, ref, "uses_linked_service")
            elif "DatasetReference" in ref_type:
                _add(src_name, ref, "uses_dataset")
            elif "DataFlowReference" in ref_type:
                _add(src_name, ref, "uses_dataflow")
            elif "PipelineReference" in ref_type:
                _add(src_name, ref, "calls_pipeline")
            elif "IntegrationRuntimeReference" in ref_type:
                _add(src_name, ref, "uses_ir")
        for v in obj.values():
            _scan_for_refs(v, src_name, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _scan_for_refs(item, src_name, depth + 1)


for comp in components:
    name  = comp["name"]
    props = comp["properties"]
    ctype = comp["type"]

    # --- Pipeline edges ---
    if ctype == "Pipeline":
        for act in props.get("activities", []):
            _scan_for_refs(act.get("typeProperties", {}), name)
            # Explicit inputs / outputs on activities
            for inp in act.get("inputs", []):
                _add(name, inp.get("referenceName", ""), "reads_dataset")
            for out in act.get("outputs", []):
                _add(name, out.get("referenceName", ""), "writes_dataset")
            # Inner activities of ForEach / IfCondition / Switch / Until
            tp = act.get("typeProperties", {})
            inner_lists = []
            if act.get("type") == "ForEach":
                inner_lists.append(tp.get("activities", []))
            elif act.get("type") == "IfCondition":
                inner_lists.append(tp.get("ifTrueActivities", []))
                inner_lists.append(tp.get("ifFalseActivities", []))
            elif act.get("type") == "Switch":
                for case in tp.get("cases", []):
                    inner_lists.append(case.get("activities", []))
                inner_lists.append(tp.get("defaultActivities", []))
            elif act.get("type") == "Until":
                inner_lists.append(tp.get("activities", []))
            for inner_acts in inner_lists:
                for child in inner_acts:
                    _scan_for_refs(child.get("typeProperties", {}), name)

    # --- DataFlow edges ---
    elif ctype == "DataFlow":
        tp = props.get("typeProperties", {})
        for src in tp.get("sources", []):
            _add(name, src.get("dataset", {}).get("referenceName", ""), "reads_dataset")
        for snk in tp.get("sinks", []):
            _add(name, snk.get("dataset", {}).get("referenceName", ""), "writes_dataset")

    # --- Dataset → LinkedService ---
    elif ctype == "Dataset":
        ls_ref = props.get("linkedServiceName", {}).get("referenceName", "")
        _add(name, ls_ref, "uses_linked_service")

    # --- LinkedService → LinkedService (Key Vault refs) ---
    elif ctype == "LinkedService":
        _scan_for_refs(props.get("typeProperties", {}), name)

    # --- Trigger → Pipeline ---
    elif ctype == "Trigger":
        for p in props.get("pipelines", []):
            _add(name, p.get("pipelineReference", {}).get("referenceName", ""), "triggers_pipeline")

# De-duplicate
edges = list({(s, t, r) for s, t, r in edges})
edges.sort()

print(f"Dependency edges found: {len(edges)}")
for s, t, r in edges:
    print(f"  {s}  --[{r}]-->  {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build NetworkX Directed Graph

# COMMAND ----------

G = nx.DiGraph()

# Add all components as nodes with attributes
for comp in components:
    G.add_node(comp["name"], component_type=comp["type"])

# Add edges (direction = "from depends on to", i.e. from needs to migrated after to)
for src, tgt, rel in edges:
    G.add_edge(src, tgt, relationship=rel)

print(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
print(f"Is DAG (no cycles): {nx.is_directed_acyclic_graph(G)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5. Component Enrichment
# MAGIC
# MAGIC Extracts rich metadata per component for LLM classification:
# MAGIC - **Activity profile**: activity types within pipelines (Copy, ExecuteDataFlow, ForEach, etc.)
# MAGIC - **Dataset details**: dataset type, linked service type, schema/table info
# MAGIC - **Linked service type**: AzureSqlDatabase, AzureBlobFS, AzureKeyVault, etc.
# MAGIC - **Naming tokens**: prefix/suffix patterns for domain inference
# MAGIC - **Domain hints**: inferred business domain from naming patterns

# COMMAND ----------

def _extract_naming_tokens(name: str) -> list[str]:
    """Split a component name into meaningful tokens for domain inference."""
    import re as _re
    # Split on camelCase, PascalCase, underscores, hyphens
    tokens = _re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    tokens = _re.split(r"[\s_\-]+", tokens)
    return [t.lower() for t in tokens if len(t) >= 2]


# Domain keyword buckets — extend as needed
_DOMAIN_KEYWORDS = {
    "finance":    {"finance", "billing", "payment", "invoice", "ledger", "accounting", "revenue", "cost"},
    "sales":      {"sales", "order", "customer", "crm", "deal", "opportunity", "revenue", "commerce"},
    "marketing":  {"marketing", "campaign", "lead", "email", "click", "impression", "audience"},
    "hr":         {"hr", "employee", "payroll", "talent", "recruit", "headcount", "workforce"},
    "supply_chain": {"supply", "inventory", "warehouse", "logistics", "shipment", "procurement"},
    "data_platform": {"master", "orchestrat", "metadata", "config", "global", "shared", "common"},
}


def _infer_domain(tokens: list[str]) -> str:
    """Match naming tokens against domain keywords. Returns best domain or empty string."""
    best_domain, best_score = "", 0
    for domain, keywords in _DOMAIN_KEYWORDS.items():
        score = sum(1 for t in tokens if any(t.startswith(k) or k.startswith(t) for k in keywords))
        if score > best_score:
            best_domain, best_score = domain, score
    return best_domain if best_score > 0 else ""


def enrich_component(comp: dict) -> dict:
    """Extract rich metadata from a component for LLM context."""
    props = comp["properties"]
    ctype = comp["type"]
    name  = comp["name"]
    meta  = {}

    tokens = _extract_naming_tokens(name)
    meta["naming_tokens"] = tokens
    meta["inferred_domain"] = _infer_domain(tokens)

    if ctype == "Pipeline":
        activities = props.get("activities", [])
        act_types = sorted({a.get("type", "") for a in activities})
        meta["activity_types"] = act_types
        meta["activity_count"] = len(activities)
        meta["has_copy"] = "Copy" in act_types
        meta["has_dataflow"] = "ExecuteDataFlow" in act_types
        meta["has_execute_pipeline"] = "ExecutePipeline" in act_types
        meta["has_control_flow"] = bool({"ForEach", "IfCondition", "Switch", "Until"} & set(act_types))
        meta["parameter_count"] = len(props.get("parameters", {}))

    elif ctype == "Dataset":
        meta["dataset_type"] = props.get("type", "")
        ls_name = props.get("linkedServiceName", {}).get("referenceName", "")
        meta["linked_service_ref"] = ls_name
        tp = props.get("typeProperties", {})
        meta["schema"] = tp.get("schema", "")
        meta["table"] = tp.get("table", "")

    elif ctype == "LinkedService":
        meta["service_type"] = props.get("type", "")

    elif ctype == "DataFlow":
        tp = props.get("typeProperties", {})
        meta["source_count"] = len(tp.get("sources", []))
        meta["sink_count"] = len(tp.get("sinks", []))
        meta["transformation_count"] = len(tp.get("transformations", []))

    elif ctype == "Trigger":
        meta["trigger_type"] = props.get("type", "")
        meta["pipeline_count"] = len(props.get("pipelines", []))

    elif ctype == "ChangeDataCapture":
        tp = props.get("typeProperties", {})
        meta["source_tables"] = [e.get("name", "") for sc in tp.get("sourceConnectionsInfo", [])
                                  for e in sc.get("sourceEntities", [])]
        meta["target_tables"] = [e.get("name", "") for tc in tp.get("targetConnectionsInfo", [])
                                  for e in tc.get("targetEntities", [])]

    return meta


# Build enrichment map for all components
comp_enrichment = {}
domain_counter = defaultdict(list)
for comp in components:
    meta = enrich_component(comp)
    comp_enrichment[comp["name"]] = meta
    d = meta.get("inferred_domain", "")
    if d:
        domain_counter[d].append(comp["name"])

print("Component enrichment complete.")
if domain_counter:
    print("Inferred domains:")
    for d, names in sorted(domain_counter.items()):
        print(f"  {d}: {', '.join(names)}")
else:
    print("No domain patterns detected from naming conventions.")

# Compute factory-wide activity similarity groups (pipelines with same activity profile)
_activity_profiles = defaultdict(list)
for comp in components:
    if comp["type"] == "Pipeline":
        profile = tuple(comp_enrichment[comp["name"]].get("activity_types", []))
        _activity_profiles[profile].append(comp["name"])

if any(len(v) > 1 for v in _activity_profiles.values()):
    print("Activity similarity groups:")
    for profile, names in _activity_profiles.items():
        if len(names) > 1:
            print(f"  {', '.join(profile)}: {', '.join(names)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Migration Phase Classification (Topological Layering)
# MAGIC
# MAGIC **Phase 1** = no dependencies (leaf nodes) → migrate first.
# MAGIC Each subsequent phase contains components whose dependencies are all
# MAGIC covered by earlier phases.

# COMMAND ----------

def topological_phases(graph: nx.DiGraph) -> dict[str, int]:
    """
    BFS-based layering on the DAG.
    Nodes with zero out-degree (no dependencies) → Phase 1.
    Then remove them, repeat.
    """
    work = graph.copy()
    phases = {}
    current_phase = 1
    while work.number_of_nodes() > 0:
        # Nodes with no outgoing edges = no unresolved dependencies
        leaves = [n for n in work.nodes() if work.out_degree(n) == 0]
        if not leaves:
            # Cycle detected — assign remaining to the next phase
            for n in work.nodes():
                phases[n] = current_phase
            break
        for n in leaves:
            phases[n] = current_phase
        work.remove_nodes_from(leaves)
        current_phase += 1
    return phases

phase_map = topological_phases(G)

# Summary
phase_groups = defaultdict(list)
for name, phase in sorted(phase_map.items(), key=lambda x: x[1]):
    phase_groups[phase].append(name)

for phase in sorted(phase_groups):
    members = phase_groups[phase]
    print(f"Phase {phase}: {len(members)} components")
    for m in members:
        ctype = comp_lookup[m]["type"] if m in comp_lookup else "?"
        deps = list(G.successors(m))
        dep_str = f" (depends on: {', '.join(deps)})" if deps else " (independent)"
        print(f"    [{ctype}] {m}{dep_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5. Migration Unit Classification
# MAGIC
# MAGIC Groups components into **migration units** for phased migration planning:
# MAGIC
# MAGIC | Migration Unit | Description | Example |
# MAGIC |---|---|---|
# MAGIC | **Single Pipeline** | Standalone pipeline with no parent/child calls | Simple ETL |
# MAGIC | **Pipeline Group** | Parent + child pipelines linked via ExecutePipeline | Orchestrator + worker pipelines |
# MAGIC | **Application** | Related pipelines + datasets forming a functional application | Sales ingestion + transform + reporting |
# MAGIC | **Domain** | Multiple applications/groups belonging to a business domain | All Finance pipelines |
# MAGIC
# MAGIC Classification uses graph connected-component analysis (rule-based) refined by the LLM.

# COMMAND ----------

# ---- Step 1: Rule-based pre-classification using graph structure ----

def _common_prefix(names: list[str]) -> str:
    """Find meaningful common prefix across pipeline names."""
    if not names or len(names) < 2:
        return ""
    prefix = names[0]
    for n in names[1:]:
        while prefix and not n.startswith(prefix):
            prefix = prefix[:-1]
    # Trim trailing separators
    prefix = prefix.rstrip("_- ")
    return prefix if len(prefix) >= 3 else ""


def rule_based_migration_units(graph: nx.DiGraph, components: list[dict], phase_map: dict) -> dict:
    """
    Assign each component a migration unit based on graph topology.

    Returns dict: component_name → {migration_unit, migration_unit_name,
                                     migration_unit_id, migration_unit_reason}
    """
    undirected = graph.to_undirected()
    ccs = list(nx.connected_components(undirected))

    unit_map = {}
    for idx, cc in enumerate(sorted(ccs, key=lambda s: min(phase_map.get(n, 99) for n in s))):
        # Categorise members of this connected component
        cc_types = defaultdict(list)
        for name in cc:
            ctype = graph.nodes[name].get("component_type", "")
            cc_types[ctype].append(name)

        pipelines      = cc_types.get("Pipeline", [])
        n_pipelines    = len(pipelines)
        n_datasets     = len(cc_types.get("Dataset", []))
        n_dataflows    = len(cc_types.get("DataFlow", []))
        n_total        = len(cc)

        # Detect parent-child pipeline calls within this CC
        pipeline_calls = [
            (s, t) for s, t, d in graph.edges(data=True)
            if d.get("relationship") == "calls_pipeline" and s in cc and t in cc
        ]

        # ---- Classification rules ----
        if n_pipelines == 0:
            unit = "Single Pipeline"
            unit_name = f"Shared Infrastructure"
            reason = "Infrastructure components (linked services, IR, credentials) not directly tied to a specific pipeline."
        elif n_pipelines == 1 and len(pipeline_calls) == 0:
            unit = "Single Pipeline"
            unit_name = pipelines[0]
            reason = f"Standalone pipeline with {n_total - 1} supporting components and no parent/child calls."
        elif n_pipelines >= 2 and len(pipeline_calls) > 0 and n_total <= 8:
            unit = "Pipeline Group"
            called = {t for _, t in pipeline_calls}
            root = [p for p in pipelines if p not in called]
            unit_name = (root[0] if root else pipelines[0]) + " Group"
            reason = f"Parent-child pipeline group with {n_pipelines} pipelines linked via ExecutePipeline: {', '.join(sorted(pipelines))}."
        elif (n_pipelines >= 2 and n_datasets >= 2) or (n_total > 8 and n_dataflows >= 1):
            unit = "Application"
            prefix = _common_prefix(sorted(pipelines))
            unit_name = (prefix + " Application") if prefix else pipelines[0] + " Application"
            reason = (
                f"Interconnected application: {n_pipelines} pipelines, {n_datasets} datasets, "
                f"{n_dataflows} data flows — forms a functional application with shared data assets."
            )
        elif n_pipelines >= 2:
            unit = "Pipeline Group"
            prefix = _common_prefix(sorted(pipelines))
            unit_name = (prefix + " Group") if prefix else pipelines[0] + " Group"
            reason = f"Pipeline group: {n_pipelines} related pipelines sharing {n_total - n_pipelines} supporting components."
        else:
            unit = "Single Pipeline"
            unit_name = pipelines[0] if pipelines else list(cc)[0]
            reason = f"Single pipeline with {n_total - 1} dependencies."

        unit_id = f"MU-{idx + 1:03d}"

        for name in cc:
            unit_map[name] = {
                "migration_unit":        unit,
                "migration_unit_name":   unit_name,
                "migration_unit_id":     unit_id,
                "migration_unit_reason": reason,
            }

    return unit_map


rule_units = rule_based_migration_units(G, components, phase_map)

# Print summary
from collections import Counter as _Counter
unit_summary = _Counter(v["migration_unit"] for v in rule_units.values())
print("Rule-based Migration Units:")
seen_ids = set()
for name in sorted(rule_units, key=lambda n: rule_units[n]["migration_unit_id"]):
    uid = rule_units[name]["migration_unit_id"]
    if uid not in seen_ids:
        seen_ids.add(uid)
        u = rule_units[name]
        members = [n for n, v in rule_units.items() if v["migration_unit_id"] == uid]
        print(f"  [{u['migration_unit_id']}] {u['migration_unit']} — \"{u['migration_unit_name']}\" ({len(members)} components)")

# COMMAND ----------

# ---- Step 2: LLM refinement of migration units ----
# Single call with the full factory context for consistent grouping.

UNIT_SYSTEM_PROMPT = """You are an Azure Data Factory migration architect. Given a factory's
complete component inventory with enriched metadata, dependency edges, and rule-based
migration-unit pre-classification, refine the migration unit assignments.

Migration Unit definitions (choose exactly one per component):
- "Single Pipeline": A standalone pipeline (simple ETL) with no parent/child pipeline calls.
  Supporting infrastructure (linked services, datasets, IR) used ONLY by this pipeline belong here.
- "Pipeline Group": A parent pipeline + its child pipelines (linked via ExecutePipeline).
  Includes their shared datasets, linked services, and triggers. Migrate as one unit.
- "Application": Multiple related pipeline groups + shared datasets that form a functional
  application (e.g., sales ingestion + transformation + reporting). Migrate together.
- "Domain": Multiple applications/groups belonging to a business domain (e.g., all Finance
  pipelines). Use this only when you can identify a clear business domain from naming patterns
  or functional purpose.

Classification signals to consider (all provided per component):
1. **Dependency graph**: Components that share dependencies → same unit.
2. **Dataset overlap**: Pipelines reading/writing the same datasets → same Application or Domain.
3. **Naming patterns**: Common prefixes/tokens suggest shared ownership (e.g., Sales_Ingest,
   Sales_Transform → "Sales" domain). Use the inferred_domain and naming_tokens fields.
4. **Activity similarity**: Pipelines with identical activity profiles (same set of activity
   types) likely serve similar purposes and may belong to the same group or domain.
5. **Domain inference**: Use the inferred_domain field as a strong hint. If 3+ pipelines share
   a domain, prefer "Domain" over "Application".

Rules:
1. Every component in the factory must be assigned exactly one migration unit.
2. Components that share dependencies should generally be in the SAME unit.
3. migration_unit must align with migration_phase — components in the same unit should be
   migrated together across their respective phases.
4. Prefer "Domain" only when there are enough pipelines (3+) with a clear business-domain theme.
5. Infrastructure shared across multiple applications (e.g., Key Vault linked service used
   everywhere) can be its own "Single Pipeline" unit labeled as shared infrastructure.
6. When naming patterns or activity similarity suggest a grouping that differs from the
   rule-based pre-classification, prefer the semantically richer grouping.

Respond with EXACTLY this JSON array (no markdown fences, no extra text):
[
  {"component_name": "<name>", "migration_unit": "<Single Pipeline|Pipeline Group|Application|Domain>", "migration_unit_name": "<descriptive name>", "migration_unit_reason": "<1 sentence explaining which signals drove the classification>"},
  ...
]
"""


def llm_classify_migration_units(components, edges, phase_map, rule_units):
    """Single LLM call to classify all components into migration units."""
    # Build compact factory summary for the LLM — enriched with all 5 classification signals
    comp_summary = []
    for c in components:
        name = c["name"]
        ru = rule_units.get(name, {})
        enrichment = comp_enrichment.get(name, {})
        entry = {
            "name": name,
            "type": c["type"],
            "phase": phase_map.get(name, 0),
            "depends_on": sorted(G.successors(name)),
            "depended_on_by": sorted(G.predecessors(name)),
            "rule_unit": ru.get("migration_unit", ""),
            "rule_unit_name": ru.get("migration_unit_name", ""),
            # Enrichment signals
            "naming_tokens": enrichment.get("naming_tokens", []),
            "inferred_domain": enrichment.get("inferred_domain", ""),
        }
        # Add type-specific metadata
        if c["type"] == "Pipeline":
            entry["activity_types"] = enrichment.get("activity_types", [])
            entry["activity_count"] = enrichment.get("activity_count", 0)
            entry["has_control_flow"] = enrichment.get("has_control_flow", False)
        elif c["type"] == "Dataset":
            entry["dataset_type"] = enrichment.get("dataset_type", "")
            entry["linked_service_ref"] = enrichment.get("linked_service_ref", "")
        elif c["type"] == "LinkedService":
            entry["service_type"] = enrichment.get("service_type", "")
        comp_summary.append(entry)

    edge_summary = [{"from": s, "to": t, "rel": r} for s, t, r in edges[:100]]

    # Activity similarity groups (pipelines with identical activity profiles)
    similarity_groups = []
    for profile, names in _activity_profiles.items():
        if len(names) > 1:
            similarity_groups.append({"activity_types": list(profile), "pipelines": names})

    user_msg = json.dumps({
        "factory_name": factory_name,
        "total_components": len(components),
        "components": comp_summary,
        "edges": edge_summary,
        "activity_similarity_groups": similarity_groups,
        "domain_groups": {d: names for d, names in domain_counter.items()},
    }, default=str)

    try:
        resp = client.predict(
            endpoint=llm_endpoint,
            inputs={
                "messages": [
                    {"role": "system", "content": UNIT_SYSTEM_PROMPT},
                    {"role": "user",   "content": user_msg},
                ],
                "max_tokens": 2000,
                "temperature": 0.05,
            },
        )
        text = resp["choices"][0]["message"]["content"].strip()
        start = text.index("[")
        end   = text.rindex("]") + 1
        llm_units = json.loads(text[start:end])

        # Build map from LLM response
        llm_map = {}
        for item in llm_units:
            cname = item.get("component_name", "")
            if cname:
                llm_map[cname] = {
                    "migration_unit":        item.get("migration_unit", ""),
                    "migration_unit_name":   item.get("migration_unit_name", ""),
                    "migration_unit_reason": item.get("migration_unit_reason", ""),
                }
        return llm_map
    except Exception as e:
        print(f"  LLM migration-unit fallback: {e}")
        return {}


client = mlflow.deployments.get_deploy_client("databricks")
print("Classifying migration units via LLM...")
llm_units = llm_classify_migration_units(components, edges, phase_map, rule_units)

# Merge: prefer LLM result, fall back to rule-based
migration_units = {}
for comp in components:
    name = comp["name"]
    if name in llm_units and llm_units[name].get("migration_unit"):
        lu = llm_units[name]
        migration_units[name] = {
            "migration_unit":        lu["migration_unit"],
            "migration_unit_name":   lu.get("migration_unit_name", rule_units[name]["migration_unit_name"]),
            "migration_unit_id":     rule_units[name]["migration_unit_id"],   # keep stable IDs from rule-based
            "migration_unit_reason": lu.get("migration_unit_reason", rule_units[name]["migration_unit_reason"]),
        }
    else:
        migration_units[name] = rule_units[name]

# Reassign unit IDs so components with the same LLM unit_name share the same ID
name_to_id = {}
id_counter = 1
for name in sorted(migration_units, key=lambda n: migration_units[n]["migration_unit_name"]):
    uname = migration_units[name]["migration_unit_name"]
    if uname not in name_to_id:
        name_to_id[uname] = f"MU-{id_counter:03d}"
        id_counter += 1
    migration_units[name]["migration_unit_id"] = name_to_id[uname]

# Build per-unit member lists (formatted as "Type:Name" for clarity)
_unit_id_members = defaultdict(list)
for n, mu in migration_units.items():
    ctype = comp_lookup[n]["type"] if n in comp_lookup else "?"
    _unit_id_members[mu["migration_unit_id"]].append(f"{ctype}:{n}")
for uid in _unit_id_members:
    _unit_id_members[uid].sort()

# Attach members string to each component's migration unit dict
for n, mu in migration_units.items():
    mu["migration_unit_members"] = ", ".join(_unit_id_members[mu["migration_unit_id"]])

# Build per-unit phase plan: "Phase 1: Type:A, Type:B | Phase 2: Type:C | ..."
_unit_id_phase_plan = {}
for uid, members in _unit_id_members.items():
    phase_buckets = defaultdict(list)
    for entry in members:
        _ctype, _cname = entry.split(":", 1)
        p = phase_map.get(_cname, 0)
        phase_buckets[p].append(entry)
    plan_parts = []
    for p in sorted(phase_buckets):
        plan_parts.append(f"Phase {p}: {', '.join(sorted(phase_buckets[p]))}")
    _unit_id_phase_plan[uid] = " | ".join(plan_parts)

for n, mu in migration_units.items():
    mu["migration_unit_phase_plan"] = _unit_id_phase_plan[mu["migration_unit_id"]]

print("\nFinal Migration Units:")
seen = set()
for name in sorted(migration_units, key=lambda n: migration_units[n]["migration_unit_id"]):
    mu = migration_units[name]
    uid = mu["migration_unit_id"]
    if uid not in seen:
        seen.add(uid)
        print(f"  [{uid}] {mu['migration_unit']} — \"{mu['migration_unit_name']}\"")
        for entry in _unit_id_members[uid]:
            ctype, cname = entry.split(":", 1)
            print(f"        Phase {phase_map.get(cname,0)}: [{ctype}] {cname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. LLM-Assisted Phase Reasoning
# MAGIC
# MAGIC The Foundation Model reviews each component's graph context and provides
# MAGIC a human-readable justification for its migration phase.

# COMMAND ----------

PHASE_SYSTEM_PROMPT = """You are an Azure Data Factory migration architect. Given an ADF
component with enriched metadata, its dependency context, and its algorithmically-assigned
migration phase, provide a concise migration-phase justification.

Consider these signals when assessing risk and providing advice:
- **Dependencies**: What this component depends on and what depends on it.
- **Activity profile**: For pipelines — activity types indicate complexity (Copy=simple,
  ForEach+IfCondition=complex orchestration, ExecuteDataFlow=Spark transformation).
- **Dataset details**: Dataset type and linked service indicate data source/sink technology.
- **Domain context**: The inferred business domain from naming patterns.
- **Migration unit context**: What other components migrate with this one.

Respond with EXACTLY this JSON (no markdown fences, no extra text):
{"phase_reason": "<1-2 sentence reason why this component belongs in this phase>", "migration_risk": "<Low|Medium|High>", "migration_notes": "<1 sentence practical advice>"}

Phase definitions:
- Phase 1: Independent foundation components (no dependencies on other ADF objects). Migrate first.
- Phase 2: Components that depend only on Phase 1 items. Migrate after Phase 1 is validated.
- Phase 3: Components that depend on Phase 1 and/or Phase 2 items. Higher-level orchestration.
- Phase 4+: Top-level orchestrators and triggers. Migrate last after all dependencies are live.
"""


def get_llm_phase_reasoning(comp_name: str, comp_type: str, phase: int,
                            deps: list[str], dependents: list[str],
                            mu: dict | None = None) -> dict:
    enrichment = comp_enrichment.get(comp_name, {})
    context = {
        "component_name": comp_name,
        "component_type": comp_type,
        "assigned_phase": phase,
        "depends_on": deps,
        "depended_on_by": dependents,
        "is_independent": len(deps) == 0,
        "migration_unit": mu.get("migration_unit", "") if mu else "",
        "migration_unit_name": mu.get("migration_unit_name", "") if mu else "",
        "inferred_domain": enrichment.get("inferred_domain", ""),
        "naming_tokens": enrichment.get("naming_tokens", []),
    }
    # Add type-specific enrichment
    if comp_type == "Pipeline":
        context["activity_types"] = enrichment.get("activity_types", [])
        context["activity_count"] = enrichment.get("activity_count", 0)
        context["has_control_flow"] = enrichment.get("has_control_flow", False)
    elif comp_type == "Dataset":
        context["dataset_type"] = enrichment.get("dataset_type", "")
        context["linked_service_ref"] = enrichment.get("linked_service_ref", "")
    elif comp_type == "LinkedService":
        context["service_type"] = enrichment.get("service_type", "")
    user_msg = json.dumps(context)
    try:
        resp = client.predict(
            endpoint=llm_endpoint,
            inputs={
                "messages": [
                    {"role": "system", "content": PHASE_SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                "max_tokens": 250,
                "temperature": 0.05,
            },
        )
        text = resp["choices"][0]["message"]["content"].strip()
        start = text.index("{")
        end = text.rindex("}") + 1
        return json.loads(text[start:end])
    except Exception as e:
        print(f"  LLM fallback for {comp_name}: {e}")
        return _fallback_reason(comp_type, phase, deps)


def _fallback_reason(comp_type: str, phase: int, deps: list[str]) -> dict:
    if phase == 1:
        reason = f"Independent {comp_type} with no ADF dependencies — safe to migrate first."
        risk = "Low"
    elif phase == 2:
        reason = f"Depends on Phase 1 components ({', '.join(deps[:3])}). Migrate after foundations are validated."
        risk = "Medium" if comp_type in ("Pipeline", "DataFlow") else "Low"
    else:
        reason = f"Phase {phase} — depends on components across earlier phases. Requires careful sequencing."
        risk = "High" if comp_type == "Pipeline" else "Medium"
    return {"phase_reason": reason, "migration_risk": risk, "migration_notes": "Review dependencies before cutover."}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run LLM Scoring

# COMMAND ----------

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
rows = []

for comp in components:
    name  = comp["name"]
    ctype = comp["type"]
    phase = phase_map.get(name, 0)
    deps       = sorted(G.successors(name))
    dependents = sorted(G.predecessors(name))
    is_independent = len(deps) == 0
    mu = migration_units.get(name, {})

    print(f"[Phase {phase}] {ctype}: {name} ({mu.get('migration_unit','?')}) ...", end=" ")
    llm = get_llm_phase_reasoning(name, ctype, phase, deps, dependents, mu)
    print(f"risk={llm.get('migration_risk', '?')}")

    enrichment = comp_enrichment.get(name, {})
    rows.append({
        "analysis_timestamp":   timestamp,
        "data_factory_name":    factory_name,
        "component_type":       ctype,
        "component_name":       name,
        "depends_on":           ", ".join(deps) if deps else "",
        "depended_on_by":       ", ".join(dependents) if dependents else "",
        "dependency_count":     len(deps),
        "dependent_count":      len(dependents),
        "is_independent":       is_independent,
        "migration_phase":      f"Phase {phase}",
        "phase_reason":         llm.get("phase_reason", ""),
        "migration_risk":       llm.get("migration_risk", ""),
        "migration_notes":      llm.get("migration_notes", ""),
        # ---- Enrichment columns ----
        "inferred_domain":      enrichment.get("inferred_domain", ""),
        "activity_types":       ", ".join(enrichment.get("activity_types", [])) if ctype == "Pipeline" else "",
        # ---- Migration Unit columns ----
        "migration_unit":             mu.get("migration_unit", ""),
        "migration_unit_name":        mu.get("migration_unit_name", ""),
        "migration_unit_id":          mu.get("migration_unit_id", ""),
        "migration_unit_members":     mu.get("migration_unit_members", ""),
        "migration_unit_phase_plan":  mu.get("migration_unit_phase_plan", ""),
        "migration_unit_reason":      mu.get("migration_unit_reason", ""),
    })

print(f"\nClassified {len(rows)} components across {max(phase_map.values())} phases.")
unit_counts = Counter(r["migration_unit"] for r in rows)
print("Migration Units: " + ", ".join(f"{k}={v}" for k, v in unit_counts.most_common()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write to Unity Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {output_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_catalog}.{output_schema}")

results_df = spark.createDataFrame(rows)
results_df.write.format("delta").mode(output_write_mode).saveAsTable(output_fqn)

written_count = spark.table(output_fqn).count()
print(f"Wrote {len(rows)} rows to {output_fqn} (mode={output_write_mode})")
print(f"Table now has {written_count} total rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Display Results

# COMMAND ----------

display(spark.table(output_fqn).orderBy("migration_phase", "component_type", "component_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Dependency Graph Visualisation
# MAGIC
# MAGIC Uses **NetworkX** (graph analysis) + **matplotlib** (rendering).
# MAGIC Three views are produced:
# MAGIC 1. **Full dependency graph** — all components, edges labelled by relationship type
# MAGIC 2. **Migration phase diagram** — nodes coloured by assigned phase
# MAGIC 3. **Pipeline-centric view** — only Pipeline and Trigger nodes

# COMMAND ----------

if render_graph:

    # ---- colour palettes ----
    TYPE_COLORS = {
        "Pipeline":           "#4A90D9",
        "Dataset":            "#50B848",
        "LinkedService":      "#F5A623",
        "DataFlow":           "#9B59B6",
        "Trigger":            "#E74C3C",
        "IntegrationRuntime": "#95A5A6",
    }
    PHASE_COLORS = {
        1: "#2ECC71",   # green  — independent
        2: "#F1C40F",   # yellow
        3: "#E67E22",   # orange
        4: "#E74C3C",   # red
        5: "#8E44AD",   # purple
    }
    EDGE_COLORS = {
        "calls_pipeline":     "#E74C3C",
        "triggers_pipeline":  "#E74C3C",
        "uses_dataflow":      "#9B59B6",
        "reads_dataset":      "#50B848",
        "writes_dataset":     "#27AE60",
        "uses_dataset":       "#50B848",
        "uses_linked_service":"#F5A623",
        "uses_ir":            "#95A5A6",
    }

    pos = nx.spring_layout(G, k=2.5, seed=42, iterations=80)

    def _draw_graph(title: str, node_color_fn, filename: str):
        fig, ax = plt.subplots(figsize=(16, 10))
        ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        node_colors = [node_color_fn(n) for n in G.nodes()]
        edge_colors = [EDGE_COLORS.get(G.edges[e].get("relationship", ""), "#CCCCCC") for e in G.edges()]

        nx.draw_networkx_nodes(G, pos, ax=ax, node_color=node_colors,
                               node_size=1800, edgecolors="#333333", linewidths=1.2)
        nx.draw_networkx_labels(G, pos, ax=ax, font_size=7, font_weight="bold")
        nx.draw_networkx_edges(G, pos, ax=ax, edge_color=edge_colors,
                               width=1.5, arrows=True, arrowsize=18,
                               connectionstyle="arc3,rad=0.1", min_source_margin=20, min_target_margin=20)

        # Edge labels
        edge_labels = {(u, v): d["relationship"].replace("_", "\n")
                       for u, v, d in G.edges(data=True)}
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels,
                                     font_size=5, font_color="#555555", ax=ax)
        ax.axis("off")
        fig.tight_layout()
        fig.savefig(f"/tmp/{filename}", dpi=150, bbox_inches="tight")
        plt.close(fig)
        return f"/tmp/{filename}"

    # ---- Graph 1: Full dependency by component type ----
    path1 = _draw_graph(
        title=f"{factory_name} — Full Dependency Graph (by Component Type)",
        node_color_fn=lambda n: TYPE_COLORS.get(G.nodes[n].get("component_type", ""), "#CCCCCC"),
        filename="dep_graph_full.png",
    )

    # ---- Graph 2: Migration phase colouring ----
    path2 = _draw_graph(
        title=f"{factory_name} — Migration Phases",
        node_color_fn=lambda n: PHASE_COLORS.get(phase_map.get(n, 1), "#CCCCCC"),
        filename="dep_graph_phases.png",
    )

    # ---- Graph 3: Pipeline-centric subgraph ----
    pipeline_types = {"Pipeline", "Trigger"}
    sub_nodes = [n for n in G.nodes() if G.nodes[n].get("component_type") in pipeline_types]
    H = G.subgraph(sub_nodes).copy()
    if H.number_of_nodes() > 0:
        pos_h = nx.spring_layout(H, k=3, seed=42)
        fig3, ax3 = plt.subplots(figsize=(12, 8))
        ax3.set_title(f"{factory_name} — Pipeline & Trigger Dependencies", fontsize=16, fontweight="bold")
        nc = [TYPE_COLORS.get(H.nodes[n].get("component_type", ""), "#CCC") for n in H.nodes()]
        ec = [EDGE_COLORS.get(H.edges[e].get("relationship", ""), "#CCC") for e in H.edges()]
        nx.draw_networkx_nodes(H, pos_h, ax=ax3, node_color=nc, node_size=2500, edgecolors="#333", linewidths=1.2)
        nx.draw_networkx_labels(H, pos_h, ax=ax3, font_size=9, font_weight="bold")
        nx.draw_networkx_edges(H, pos_h, ax=ax3, edge_color=ec, width=2, arrows=True, arrowsize=22,
                               connectionstyle="arc3,rad=0.12", min_source_margin=25, min_target_margin=25)
        edge_labels_h = {(u, v): d["relationship"].replace("_", "\n") for u, v, d in H.edges(data=True)}
        nx.draw_networkx_edge_labels(H, pos_h, edge_labels=edge_labels_h, font_size=7, ax=ax3)
        ax3.axis("off")
        fig3.tight_layout()
        path3 = "/tmp/dep_graph_pipelines.png"
        fig3.savefig(path3, dpi=150, bbox_inches="tight")
        plt.close(fig3)
    else:
        path3 = None

    print("Graph images saved. Displaying below...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Graph 1 — Full Dependency Graph (by Component Type)

# COMMAND ----------

if render_graph:
    displayHTML(f'<img src="data:image/png;base64,{__import__("base64").b64encode(open(path1,"rb").read()).decode()}" style="max-width:100%"/>')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Graph 2 — Migration Phase Diagram

# COMMAND ----------

if render_graph:
    displayHTML(f'<img src="data:image/png;base64,{__import__("base64").b64encode(open(path2,"rb").read()).decode()}" style="max-width:100%"/>')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Graph 3 — Pipeline & Trigger View

# COMMAND ----------

if render_graph and path3:
    displayHTML(f'<img src="data:image/png;base64,{__import__("base64").b64encode(open(path3,"rb").read()).decode()}" style="max-width:100%"/>')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Legend

# COMMAND ----------

if render_graph:
    legend_html = """
    <div style="display:flex;gap:30px;flex-wrap:wrap;font-family:sans-serif;font-size:13px;">
      <div>
        <b>Component Types</b><br/>
        <span style="color:#4A90D9">&#9632;</span> Pipeline &nbsp;
        <span style="color:#50B848">&#9632;</span> Dataset &nbsp;
        <span style="color:#F5A623">&#9632;</span> LinkedService &nbsp;
        <span style="color:#9B59B6">&#9632;</span> DataFlow &nbsp;
        <span style="color:#E74C3C">&#9632;</span> Trigger &nbsp;
        <span style="color:#95A5A6">&#9632;</span> IntegrationRuntime
      </div>
      <div>
        <b>Migration Phases</b><br/>
        <span style="color:#2ECC71">&#9632;</span> Phase 1 (independent) &nbsp;
        <span style="color:#F1C40F">&#9632;</span> Phase 2 &nbsp;
        <span style="color:#E67E22">&#9632;</span> Phase 3 &nbsp;
        <span style="color:#E74C3C">&#9632;</span> Phase 4 &nbsp;
        <span style="color:#8E44AD">&#9632;</span> Phase 5+
      </div>
    </div>
    """
    displayHTML(legend_html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

from collections import Counter

phase_counter = Counter(r["migration_phase"] for r in rows)
risk_counter  = Counter(r["migration_risk"] for r in rows)
unit_counter  = Counter(r["migration_unit"] for r in rows)
indep_count   = sum(1 for r in rows if r["is_independent"])

print("=" * 75)
print(f"DEPENDENCY ANALYSIS SUMMARY — {factory_name}")
print("=" * 75)
print(f"Total components:  {len(rows)}")
print(f"Total edges:       {len(edges)}")
print(f"Independent:       {indep_count}")
print(f"Graph is DAG:      {nx.is_directed_acyclic_graph(G)}")
print()
print("Migration Phases:")
for phase in sorted(phase_counter):
    members = [r["component_name"] for r in rows if r["migration_phase"] == phase]
    print(f"  {phase}: {len(members)} components — {', '.join(members)}")
print()
print("Migration Risk:")
for risk in ["Low", "Medium", "High"]:
    print(f"  {risk:8s}: {risk_counter.get(risk, 0)}")
print()
print("Migration Unit Breakdown:")
for unit_type in ["Single Pipeline", "Pipeline Group", "Application", "Domain"]:
    cnt = unit_counter.get(unit_type, 0)
    if cnt > 0:
        print(f"  {unit_type:18s}: {cnt} components")
print()
print("Migration Units (detailed):")
seen_ids = set()
for r in sorted(rows, key=lambda x: x["migration_unit_id"]):
    uid = r["migration_unit_id"]
    if uid not in seen_ids:
        seen_ids.add(uid)
        members = [x for x in rows if x["migration_unit_id"] == uid]
        phases  = sorted({x["migration_phase"] for x in members})
        print(f"  [{uid}] {r['migration_unit']} — \"{r['migration_unit_name']}\"")
        print(f"         {len(members)} components | Phases: {', '.join(phases)}")
        print(f"         Reason: {r['migration_unit_reason']}")
print()
print(f"Results written to: {output_fqn}")
