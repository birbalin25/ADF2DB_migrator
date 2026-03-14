"""
Local tests for the dependency-analysis logic.

Validates dependency extraction, graph construction, phase classification,
and migration unit assignment against the sample ARM template — no Spark
or Databricks required.

Run:  pytest tests/test_dependency_analyzer.py -v
"""

import json
import re
from collections import defaultdict
from pathlib import Path

import pytest

# ---- Replicate core functions from the notebook ----

TYPE_MAP = {
    "Microsoft.DataFactory/factories/pipelines":           "Pipeline",
    "Microsoft.DataFactory/factories/datasets":            "Dataset",
    "Microsoft.DataFactory/factories/linkedservices":      "LinkedService",
    "Microsoft.DataFactory/factories/triggers":            "Trigger",
    "Microsoft.DataFactory/factories/dataflows":           "DataFlow",
    "Microsoft.DataFactory/factories/integrationruntimes": "IntegrationRuntime",
    "Microsoft.DataFactory/factories/adfcdcs":             "ChangeDataCapture",
    "Microsoft.DataFactory/factories/credentials":         "Credential",
    "Microsoft.DataFactory/factories/globalParameters":    "GlobalParameter",
    "Microsoft.DataFactory/factories/managedVirtualNetworks":                            "ManagedVirtualNetwork",
    "Microsoft.DataFactory/factories/managedVirtualNetworks/managedPrivateEndpoints":    "ManagedPrivateEndpoint",
    "Microsoft.DataFactory/factories/privateEndpointConnections":                        "PrivateEndpointConnection",
}

def _extract_name(resource):
    raw = resource.get("name") or resource.get("Name") or "unknown"
    m = re.search(r"'([^']+)'\s*\)\s*\]$", raw)
    if m:
        path = m.group(1).lstrip("/")
        return path.rsplit("/", 1)[-1] if "/" in path else path
    if "/" in raw:
        return raw.rsplit("/", 1)[-1]
    return raw

def _props(resource):
    return resource.get("properties") or resource.get("Properties") or {}

def parse_arm_template(template):
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


def extract_edges(components):
    component_names = {c["name"] for c in components}
    edges = []

    def _add(src, tgt, rel):
        if tgt and tgt in component_names:
            edges.append((src, tgt, rel))

    def _scan(obj, src_name, depth=0):
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
                _scan(v, src_name, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                _scan(item, src_name, depth + 1)

    for comp in components:
        name = comp["name"]
        props = comp["properties"]
        ctype = comp["type"]

        if ctype == "Pipeline":
            for act in props.get("activities", []):
                _scan(act.get("typeProperties", {}), name)
                for inp in act.get("inputs", []):
                    _add(name, inp.get("referenceName", ""), "reads_dataset")
                for out in act.get("outputs", []):
                    _add(name, out.get("referenceName", ""), "writes_dataset")
                tp = act.get("typeProperties", {})
                inner_lists = []
                if act.get("type") == "ForEach":
                    inner_lists.append(tp.get("activities", []))
                elif act.get("type") == "IfCondition":
                    inner_lists.append(tp.get("ifTrueActivities", []))
                    inner_lists.append(tp.get("ifFalseActivities", []))
                for inner_acts in inner_lists:
                    for child in inner_acts:
                        _scan(child.get("typeProperties", {}), name)
        elif ctype == "DataFlow":
            tp = props.get("typeProperties", {})
            for src in tp.get("sources", []):
                _add(name, src.get("dataset", {}).get("referenceName", ""), "reads_dataset")
            for snk in tp.get("sinks", []):
                _add(name, snk.get("dataset", {}).get("referenceName", ""), "writes_dataset")
        elif ctype == "Dataset":
            ls_ref = props.get("linkedServiceName", {}).get("referenceName", "")
            _add(name, ls_ref, "uses_linked_service")
        elif ctype == "LinkedService":
            _scan(props.get("typeProperties", {}), name)
        elif ctype == "Trigger":
            for p in props.get("pipelines", []):
                _add(name, p.get("pipelineReference", {}).get("referenceName", ""), "triggers_pipeline")

    return list({(s, t, r) for s, t, r in edges})


def topological_phases(nodes, edges):
    """Pure-Python phase classification (no networkx needed for test)."""
    # Build adjacency (out-edges = dependencies)
    out_edges = defaultdict(set)
    for s, t, _ in edges:
        out_edges[s].add(t)

    remaining = set(nodes)
    phases = {}
    current = 1
    while remaining:
        # Nodes whose all dependencies are already phased
        leaves = {n for n in remaining if not (out_edges[n] & remaining)}
        if not leaves:
            for n in remaining:
                phases[n] = current
            break
        for n in leaves:
            phases[n] = current
        remaining -= leaves
        current += 1
    return phases


# ---- Fixtures ----

SAMPLE = Path(__file__).parent / "sample_arm_template.json"

@pytest.fixture(scope="module")
def arm():
    with open(SAMPLE) as f:
        return json.load(f)

@pytest.fixture(scope="module")
def parsed(arm):
    return parse_arm_template(arm)

@pytest.fixture(scope="module")
def edge_list(parsed):
    _, comps = parsed
    return extract_edges(comps)


# ---- Tests ----

class TestEdgeExtraction:

    def test_trigger_to_pipeline_edges(self, edge_list):
        trigger_edges = [(s, t) for s, t, r in edge_list if r == "triggers_pipeline"]
        assert ("DailySchedule", "SalesDataPipeline") in trigger_edges
        assert ("WeeklyOrchestrator", "MasterOrchestrator") in trigger_edges

    def test_pipeline_calls_pipeline(self, edge_list):
        call_edges = [(s, t) for s, t, r in edge_list if r == "calls_pipeline"]
        assert ("MasterOrchestrator", "SalesDataPipeline") in call_edges

    def test_pipeline_reads_dataset(self, edge_list):
        read_edges = [(s, t) for s, t, r in edge_list if r == "reads_dataset"]
        assert ("SalesDataPipeline", "AzureSqlTransactions") in read_edges

    def test_pipeline_writes_dataset(self, edge_list):
        write_edges = [(s, t) for s, t, r in edge_list if r == "writes_dataset"]
        assert ("SalesDataPipeline", "AdlsTransactionsParquet") in write_edges

    def test_dataset_to_linked_service(self, edge_list):
        ls_edges = [(s, t) for s, t, r in edge_list if r == "uses_linked_service"]
        assert ("AzureSqlTransactions", "AzureSqlLinkedService") in ls_edges
        assert ("AdlsTransactionsParquet", "AdlsLinkedService") in ls_edges

    def test_dataflow_reads_dataset(self, edge_list):
        df_reads = [(s, t) for s, t, r in edge_list if s == "SalesTransformFlow" and r == "reads_dataset"]
        assert ("SalesTransformFlow", "AzureSqlTransactions") in df_reads

    def test_pipeline_uses_dataflow(self, edge_list):
        df_edges = [(s, t) for s, t, r in edge_list if r == "uses_dataflow"]
        assert ("SalesDataPipeline", "SalesTransformFlow") in df_edges

    def test_pipeline_uses_ir(self, edge_list):
        ir_edges = [(s, t) for s, t, r in edge_list if r == "uses_ir"]
        assert ("SalesDataPipeline", "AutoResolveIntegrationRuntime") in ir_edges

    def test_linked_service_references_keyvault(self, edge_list):
        """AzureSqlLinkedService and AdlsLinkedService reference AzureKeyVaultLinkedService."""
        kv_edges = [(s, t) for s, t, r in edge_list if t == "AzureKeyVaultLinkedService"]
        sources = {s for s, _ in kv_edges}
        assert "AzureSqlLinkedService" in sources
        assert "AdlsLinkedService" in sources

    def test_no_self_loops(self, edge_list):
        self_loops = [(s, t) for s, t, _ in edge_list if s == t]
        assert len(self_loops) == 0


class TestPhaseClassification:

    def test_independent_components_are_phase1(self, parsed, edge_list):
        _, comps = parsed
        names = [c["name"] for c in comps]
        phases = topological_phases(names, edge_list)
        # AzureKeyVaultLinkedService and AutoResolveIntegrationRuntime have no ADF dependencies
        assert phases["AzureKeyVaultLinkedService"] == 1
        assert phases["AutoResolveIntegrationRuntime"] == 1

    def test_triggers_are_last_phase(self, parsed, edge_list):
        _, comps = parsed
        names = [c["name"] for c in comps]
        phases = topological_phases(names, edge_list)
        max_phase = max(phases.values())
        # Triggers depend on pipelines, so they should be in the last or near-last phase
        assert phases["DailySchedule"] >= phases["SalesDataPipeline"]
        assert phases["WeeklyOrchestrator"] >= phases["MasterOrchestrator"]

    def test_master_after_sales(self, parsed, edge_list):
        _, comps = parsed
        names = [c["name"] for c in comps]
        phases = topological_phases(names, edge_list)
        # MasterOrchestrator calls SalesDataPipeline → must be later phase
        assert phases["MasterOrchestrator"] > phases["SalesDataPipeline"]

    def test_datasets_before_pipelines(self, parsed, edge_list):
        _, comps = parsed
        names = [c["name"] for c in comps]
        phases = topological_phases(names, edge_list)
        # Datasets are dependencies of pipelines
        assert phases["AzureSqlTransactions"] < phases["SalesDataPipeline"]
        assert phases["AdlsTransactionsParquet"] < phases["SalesDataPipeline"]

    def test_all_components_assigned(self, parsed, edge_list):
        _, comps = parsed
        names = [c["name"] for c in comps]
        phases = topological_phases(names, edge_list)
        assert set(phases.keys()) == set(names)

    def test_phase_numbers_start_at_1(self, parsed, edge_list):
        _, comps = parsed
        names = [c["name"] for c in comps]
        phases = topological_phases(names, edge_list)
        assert min(phases.values()) == 1


# ---- Component Enrichment helpers (mirror notebook Section 3.5) ----

def _extract_naming_tokens(name):
    tokens = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    tokens = re.split(r"[\s_\-]+", tokens)
    return [t.lower() for t in tokens if len(t) >= 2]


_DOMAIN_KEYWORDS = {
    "finance":    {"finance", "billing", "payment", "invoice", "ledger", "accounting", "revenue", "cost"},
    "sales":      {"sales", "order", "customer", "crm", "deal", "opportunity", "revenue", "commerce"},
    "marketing":  {"marketing", "campaign", "lead", "email", "click", "impression", "audience"},
    "hr":         {"hr", "employee", "payroll", "talent", "recruit", "headcount", "workforce"},
    "supply_chain": {"supply", "inventory", "warehouse", "logistics", "shipment", "procurement"},
    "data_platform": {"master", "orchestrat", "metadata", "config", "global", "shared", "common"},
}


def _infer_domain(tokens):
    best_domain, best_score = "", 0
    for domain, keywords in _DOMAIN_KEYWORDS.items():
        score = sum(1 for t in tokens if any(t.startswith(k) or k.startswith(t) for k in keywords))
        if score > best_score:
            best_domain, best_score = domain, score
    return best_domain if best_score > 0 else ""


def enrich_component(comp):
    props = comp["properties"]
    ctype = comp["type"]
    name = comp["name"]
    meta = {}
    tokens = _extract_naming_tokens(name)
    meta["naming_tokens"] = tokens
    meta["inferred_domain"] = _infer_domain(tokens)
    if ctype == "Pipeline":
        activities = props.get("activities", [])
        meta["activity_types"] = sorted({a.get("type", "") for a in activities})
    return meta


# ---- Migration Unit helpers (pure-Python, no networkx) ----

def _connected_components(nodes, edges):
    """BFS-based connected-components on the undirected view of a directed edge list."""
    adj = defaultdict(set)
    for s, t, _ in edges:
        adj[s].add(t)
        adj[t].add(s)
    visited = set()
    components = []
    for node in nodes:
        if node in visited:
            continue
        # BFS
        queue = [node]
        cc = set()
        while queue:
            n = queue.pop()
            if n in visited:
                continue
            visited.add(n)
            cc.add(n)
            for nb in adj[n]:
                if nb not in visited and nb in set(nodes):
                    queue.append(nb)
        components.append(cc)
    return components


def _common_prefix(names):
    if not names or len(names) < 2:
        return ""
    prefix = names[0]
    for n in names[1:]:
        while prefix and not n.startswith(prefix):
            prefix = prefix[:-1]
    prefix = prefix.rstrip("_- ")
    return prefix if len(prefix) >= 3 else ""


def rule_based_migration_units(nodes, edges, comp_type_map, phase_map):
    """
    Pure-Python equivalent of the notebook's rule_based_migration_units.
    nodes: list of component names
    edges: list of (src, tgt, rel) tuples
    comp_type_map: dict name → component type
    phase_map: dict name → phase number
    """
    ccs = _connected_components(nodes, edges)

    unit_map = {}
    for idx, cc in enumerate(sorted(ccs, key=lambda s: min(phase_map.get(n, 99) for n in s))):
        cc_types = defaultdict(list)
        for name in cc:
            cc_types[comp_type_map.get(name, "")].append(name)

        pipelines = cc_types.get("Pipeline", [])
        n_pipelines = len(pipelines)
        n_datasets = len(cc_types.get("Dataset", []))
        n_dataflows = len(cc_types.get("DataFlow", []))
        n_total = len(cc)

        # Detect parent-child calls within this CC
        pipeline_calls = [
            (s, t) for s, t, r in edges
            if r == "calls_pipeline" and s in cc and t in cc
        ]

        if n_pipelines == 0:
            unit = "Single Pipeline"
            unit_name = "Shared Infrastructure"
            reason = "Infrastructure components not directly tied to a specific pipeline."
        elif n_pipelines == 1 and len(pipeline_calls) == 0:
            unit = "Single Pipeline"
            unit_name = pipelines[0]
            reason = f"Standalone pipeline with {n_total - 1} supporting components."
        elif n_pipelines >= 2 and len(pipeline_calls) > 0 and n_total <= 8:
            unit = "Pipeline Group"
            called = {t for _, t in pipeline_calls}
            root = [p for p in pipelines if p not in called]
            unit_name = (root[0] if root else pipelines[0]) + " Group"
            reason = f"Parent-child pipeline group with {n_pipelines} pipelines."
        elif (n_pipelines >= 2 and n_datasets >= 2) or (n_total > 8 and n_dataflows >= 1):
            unit = "Application"
            prefix = _common_prefix(sorted(pipelines))
            unit_name = (prefix + " Application") if prefix else pipelines[0] + " Application"
            reason = f"Interconnected application: {n_pipelines} pipelines, {n_datasets} datasets."
        elif n_pipelines >= 2:
            unit = "Pipeline Group"
            prefix = _common_prefix(sorted(pipelines))
            unit_name = (prefix + " Group") if prefix else pipelines[0] + " Group"
            reason = f"Pipeline group: {n_pipelines} related pipelines."
        else:
            unit = "Single Pipeline"
            unit_name = pipelines[0] if pipelines else list(cc)[0]
            reason = f"Single pipeline with {n_total - 1} dependencies."

        unit_id = f"MU-{idx + 1:03d}"
        for name in cc:
            unit_map[name] = {
                "migration_unit": unit,
                "migration_unit_name": unit_name,
                "migration_unit_id": unit_id,
                "migration_unit_reason": reason,
            }

    # Build per-unit member lists (formatted as "Type:Name")
    uid_members = defaultdict(list)
    for n, mu in unit_map.items():
        uid_members[mu["migration_unit_id"]].append(f"{comp_type_map.get(n, '?')}:{n}")
    for uid in uid_members:
        uid_members[uid].sort()
    for n, mu in unit_map.items():
        mu["migration_unit_members"] = ", ".join(uid_members[mu["migration_unit_id"]])

    # Build per-unit phase plan
    uid_phase_plan = {}
    for uid, members in uid_members.items():
        phase_buckets = defaultdict(list)
        for entry in members:
            _ctype, _cname = entry.split(":", 1)
            p = phase_map.get(_cname, 0)
            phase_buckets[p].append(entry)
        plan_parts = []
        for p in sorted(phase_buckets):
            plan_parts.append(f"Phase {p}: {', '.join(sorted(phase_buckets[p]))}")
        uid_phase_plan[uid] = " | ".join(plan_parts)
    for n, mu in unit_map.items():
        mu["migration_unit_phase_plan"] = uid_phase_plan[mu["migration_unit_id"]]

    return unit_map


# ---- Migration unit fixtures ----

@pytest.fixture(scope="module")
def comp_type_map(parsed):
    _, comps = parsed
    return {c["name"]: c["type"] for c in comps}


@pytest.fixture(scope="module")
def phase_map(parsed, edge_list):
    _, comps = parsed
    names = [c["name"] for c in comps]
    return topological_phases(names, edge_list)


@pytest.fixture(scope="module")
def migration_units(parsed, edge_list, comp_type_map, phase_map):
    _, comps = parsed
    names = [c["name"] for c in comps]
    return rule_based_migration_units(names, edge_list, comp_type_map, phase_map)


@pytest.fixture(scope="module")
def enrichment_map(parsed):
    _, comps = parsed
    return {c["name"]: enrich_component(c) for c in comps}


# ---- Migration Unit Tests ----

class TestMigrationUnits:

    def test_all_components_have_unit(self, parsed, migration_units):
        _, comps = parsed
        names = {c["name"] for c in comps}
        assert set(migration_units.keys()) == names

    def test_unit_fields_present(self, migration_units):
        required_fields = {"migration_unit", "migration_unit_name", "migration_unit_id",
                           "migration_unit_members", "migration_unit_phase_plan",
                           "migration_unit_reason"}
        for name, mu in migration_units.items():
            assert required_fields.issubset(mu.keys()), f"Missing fields for {name}"
            for field in required_fields:
                assert mu[field], f"Empty {field} for {name}"

    def test_main_cluster_is_application(self, migration_units):
        """The big connected component (2 pipelines, 2 datasets, 1 dataflow, etc.) → Application."""
        assert migration_units["SalesDataPipeline"]["migration_unit"] == "Application"
        assert migration_units["MasterOrchestrator"]["migration_unit"] == "Application"

    def test_application_contains_pipelines_and_datasets(self, migration_units):
        app_id = migration_units["SalesDataPipeline"]["migration_unit_id"]
        app_members = [n for n, mu in migration_units.items() if mu["migration_unit_id"] == app_id]
        assert "SalesDataPipeline" in app_members
        assert "MasterOrchestrator" in app_members
        assert "AzureSqlTransactions" in app_members
        assert "AdlsTransactionsParquet" in app_members
        assert "SalesTransformFlow" in app_members

    def test_application_includes_triggers(self, migration_units):
        """Triggers connected to the main pipelines should be in the same unit."""
        app_id = migration_units["SalesDataPipeline"]["migration_unit_id"]
        assert migration_units["DailySchedule"]["migration_unit_id"] == app_id
        assert migration_units["WeeklyOrchestrator"]["migration_unit_id"] == app_id

    def test_application_includes_linked_services(self, migration_units):
        app_id = migration_units["SalesDataPipeline"]["migration_unit_id"]
        assert migration_units["AzureKeyVaultLinkedService"]["migration_unit_id"] == app_id
        assert migration_units["AzureSqlLinkedService"]["migration_unit_id"] == app_id
        assert migration_units["AdlsLinkedService"]["migration_unit_id"] == app_id

    def test_isolated_components_are_shared_infra(self, migration_units):
        """Components with no edges (CDC, Credential, VNet, Private Endpoint) → Shared Infrastructure."""
        for name in ["SalesCDC", "ManagedIdentityCredential", "default", "AzureSqlPrivateEndpoint"]:
            mu = migration_units[name]
            assert mu["migration_unit"] == "Single Pipeline", f"{name} expected Single Pipeline, got {mu['migration_unit']}"
            assert mu["migration_unit_name"] == "Shared Infrastructure", f"{name} expected Shared Infrastructure"

    def test_same_cc_same_unit_id(self, migration_units):
        """All components in the main connected component share the same unit_id."""
        main_members = ["SalesDataPipeline", "MasterOrchestrator", "DailySchedule",
                        "AzureSqlTransactions", "AdlsTransactionsParquet", "SalesTransformFlow"]
        ids = {migration_units[m]["migration_unit_id"] for m in main_members}
        assert len(ids) == 1, f"Expected 1 unit_id for main cluster, got {ids}"

    def test_different_cc_different_unit_id(self, migration_units):
        """Disconnected components should have different unit_ids from the main cluster."""
        main_id = migration_units["SalesDataPipeline"]["migration_unit_id"]
        for name in ["SalesCDC", "ManagedIdentityCredential"]:
            assert migration_units[name]["migration_unit_id"] != main_id, \
                f"{name} should have a different unit_id from main cluster"

    def test_unit_id_format(self, migration_units):
        """All unit IDs follow the MU-NNN pattern."""
        import re as _re
        for name, mu in migration_units.items():
            assert _re.match(r"^MU-\d{3}$", mu["migration_unit_id"]), \
                f"Bad unit_id format for {name}: {mu['migration_unit_id']}"

    def test_valid_unit_types(self, migration_units):
        valid = {"Single Pipeline", "Pipeline Group", "Application", "Domain"}
        for name, mu in migration_units.items():
            assert mu["migration_unit"] in valid, \
                f"Invalid unit type for {name}: {mu['migration_unit']}"

    def test_members_list_pipelines_in_application(self, migration_units):
        """migration_unit_members for the Application should list all related components with types."""
        members_str = migration_units["SalesDataPipeline"]["migration_unit_members"]
        assert "Pipeline:SalesDataPipeline" in members_str
        assert "Pipeline:MasterOrchestrator" in members_str
        assert "Dataset:AzureSqlTransactions" in members_str
        assert "DataFlow:SalesTransformFlow" in members_str
        assert "Trigger:DailySchedule" in members_str
        assert "LinkedService:AzureKeyVaultLinkedService" in members_str

    def test_members_same_within_unit(self, migration_units):
        """All components in the same unit should have identical migration_unit_members."""
        app_id = migration_units["SalesDataPipeline"]["migration_unit_id"]
        app_components = [n for n, mu in migration_units.items() if mu["migration_unit_id"] == app_id]
        members_values = {migration_units[n]["migration_unit_members"] for n in app_components}
        assert len(members_values) == 1, "All components in the same unit should share the same members list"

    def test_isolated_members_only_self(self, migration_units):
        """Isolated components sharing 'Shared Infrastructure' should list all infra components."""
        members_str = migration_units["ManagedIdentityCredential"]["migration_unit_members"]
        # Isolated components with the same unit_name share a unit, so members includes all of them
        assert "Credential:ManagedIdentityCredential" in members_str

    def test_phase_plan_has_pipe_delimited_phases(self, migration_units):
        """Phase plan uses ' | ' to separate phases."""
        plan = migration_units["SalesDataPipeline"]["migration_unit_phase_plan"]
        assert "Phase 1:" in plan
        assert " | " in plan

    def test_phase_plan_ordering(self, migration_units):
        """Phase plan lists phases in ascending order."""
        plan = migration_units["SalesDataPipeline"]["migration_unit_phase_plan"]
        import re as _re
        phase_nums = [int(x) for x in _re.findall(r"Phase (\d+):", plan)]
        assert phase_nums == sorted(phase_nums), f"Phases not sorted: {phase_nums}"
        assert phase_nums[0] == 1, "Should start with Phase 1"

    def test_phase_plan_contains_components(self, migration_units):
        """Phase plan includes Type:Name entries for each phase."""
        plan = migration_units["SalesDataPipeline"]["migration_unit_phase_plan"]
        # KeyVault and IR are phase 1 (independent)
        assert "LinkedService:AzureKeyVaultLinkedService" in plan
        assert "IntegrationRuntime:AutoResolveIntegrationRuntime" in plan
        # Pipelines should be in a later phase
        assert "Pipeline:SalesDataPipeline" in plan
        assert "Pipeline:MasterOrchestrator" in plan

    def test_phase_plan_same_within_unit(self, migration_units):
        """All components in the same unit share the same phase plan."""
        app_id = migration_units["SalesDataPipeline"]["migration_unit_id"]
        app_components = [n for n, mu in migration_units.items() if mu["migration_unit_id"] == app_id]
        plans = {migration_units[n]["migration_unit_phase_plan"] for n in app_components}
        assert len(plans) == 1, "All components in the same unit should share the same phase plan"

    def test_phase_plan_pipeline_after_dataset(self, migration_units):
        """In the phase plan, datasets appear in an earlier phase than pipelines."""
        plan = migration_units["SalesDataPipeline"]["migration_unit_phase_plan"]
        # Split by phase segments and find which phase contains each component
        import re as _re
        ds_phase = None
        pl_phase = None
        for segment in plan.split(" | "):
            m = _re.match(r"Phase (\d+):", segment)
            if not m:
                continue
            phase_num = int(m.group(1))
            if "Dataset:AzureSqlTransactions" in segment:
                ds_phase = phase_num
            if "Pipeline:SalesDataPipeline" in segment:
                pl_phase = phase_num
        assert ds_phase is not None and pl_phase is not None, \
            "Both dataset and pipeline should be in the plan"
        assert ds_phase < pl_phase, \
            f"Datasets (Phase {ds_phase}) should be in an earlier phase than pipelines (Phase {pl_phase})"


class TestEnrichmentColumns:
    """Tests for the enrichment columns (inferred_domain, activity_types)."""

    def test_sales_pipeline_domain(self, enrichment_map):
        meta = enrichment_map["SalesDataPipeline"]
        assert meta["inferred_domain"] == "sales"

    def test_master_orchestrator_domain(self, enrichment_map):
        meta = enrichment_map["MasterOrchestrator"]
        assert meta["inferred_domain"] == "data_platform"

    def test_pipeline_has_activity_types(self, enrichment_map):
        meta = enrichment_map["SalesDataPipeline"]
        assert "activity_types" in meta
        assert isinstance(meta["activity_types"], list)
        assert len(meta["activity_types"]) > 0

    def test_non_pipeline_has_no_activity_types(self, enrichment_map):
        meta = enrichment_map["AzureSqlTransactions"]
        assert "activity_types" not in meta

    def test_sales_pipeline_activity_types(self, enrichment_map):
        meta = enrichment_map["SalesDataPipeline"]
        assert "Copy" in meta["activity_types"]
        assert "ExecuteDataFlow" in meta["activity_types"]

    def test_orchestrator_activity_types(self, enrichment_map):
        meta = enrichment_map["MasterOrchestrator"]
        assert "ForEach" in meta["activity_types"]
        assert "IfCondition" in meta["activity_types"]

    def test_naming_tokens_extracted(self, enrichment_map):
        tokens = enrichment_map["SalesDataPipeline"]["naming_tokens"]
        assert "sales" in tokens
        assert "data" in tokens
        assert "pipeline" in tokens

    def test_all_components_have_enrichment(self, parsed, enrichment_map):
        _, comps = parsed
        for c in comps:
            assert c["name"] in enrichment_map
            meta = enrichment_map[c["name"]]
            assert "naming_tokens" in meta
            assert "inferred_domain" in meta
