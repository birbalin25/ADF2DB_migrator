"""
Tests for the ADF → Databricks AI Code Converter logic.

Validates all pure-Python helper functions from notebook 03 against
the sample ARM template and synthetic test data — no Spark, Databricks,
or LLM dependencies required.

Run:  pytest tests/test_code_converter.py -v
"""

import json
import re
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Replicate core functions from the notebook (pure Python, no Spark/dbutils)
# ---------------------------------------------------------------------------

SKIP_TYPES = {"Parameter", "Variable", "GlobalParameter", "Credential", "ManagedVirtualNetwork", "ManagedPrivateEndpoint"}

SKIP_REASONS = {
    "Parameter": "Mapped to Workflow job parameters / dbutils.widgets.get(). Captured in Pipeline YAML.",
    "Variable": "Mapped to dbutils.jobs.taskValues / Python variables. Captured in notebook code.",
    "GlobalParameter": "Mapped to Workflow-level parameters or shared config. Captured in Pipeline YAML.",
    "Credential": "Mapped to UC Storage Credential / Secret Scope. Manual setup — provide instructions in run_instructions.",
    "ManagedVirtualNetwork": "Infrastructure config. Manual setup step — configure Databricks VNet injection.",
    "ManagedPrivateEndpoint": "Infrastructure config. Manual setup step — configure Databricks Private Link.",
}

COMPLEXITY_TOKEN_MAP = {"Low": 1000, "Medium": 2000, "High": 3000}


def should_generate_code(component_type: str) -> bool:
    return component_type not in SKIP_TYPES


def get_skip_reason(component_type: str) -> str:
    return SKIP_REASONS.get(component_type, f"No code generation needed for {component_type}.")


def get_max_tokens(complexity: str) -> int:
    return COMPLEXITY_TOKEN_MAP.get(complexity, 2000)


def build_arm_lookup(raw_json: dict) -> dict:
    lookup = {}
    for r in raw_json.get("resources", []):
        raw_name = r.get("name") or r.get("Name") or ""
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
    dep_index = {}
    for d in dep_rows:
        key = (d.get("data_factory_name", ""), d.get("component_name", ""))
        dep_index[key] = d

    merged = []
    for m in mapping_rows:
        factory = m.get("data_factory_name", "")
        name = m.get("adf_component_name", "")
        dep = dep_index.get((factory, name), {})

        if not dep and m.get("parent_component", ""):
            parent_name = m["parent_component"].split("/")[0]
            dep = dep_index.get((factory, parent_name), {})

        merged.append({
            "data_factory_name": factory,
            "adf_component_type": m.get("adf_component_type", ""),
            "adf_component_name": name,
            "adf_subtype": m.get("adf_subtype", ""),
            "parent_component": m.get("parent_component", ""),
            "databricks_equivalent": m.get("databricks_equivalent", ""),
            "complexity_score": m.get("complexity_score", "Low"),
            "adf_config_summary": m.get("adf_config_summary", ""),
            "key_metrics": m.get("key_metrics", "{}"),
            "migration_phase": dep.get("migration_phase", ""),
            "migration_unit_id": dep.get("migration_unit_id", ""),
            "migration_unit_name": dep.get("migration_unit_name", ""),
            "depends_on": dep.get("depends_on", ""),
            "depended_on_by": dep.get("depended_on_by", ""),
            "migration_unit_phase_plan": dep.get("migration_unit_phase_plan", ""),
        })
    return merged


def apply_phase_filter(components: list, phase_filter: str) -> list:
    if phase_filter.strip().lower() == "all":
        return components
    target = phase_filter.strip().lower()
    return [c for c in components if c.get("migration_phase", "").strip().lower() == target]


def compute_execution_order(phase_plan_str: str) -> dict:
    order_map = {}
    counter = 1
    if not phase_plan_str:
        return order_map
    for segment in phase_plan_str.split(" | "):
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
    text = text.strip()
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        return json.loads(text[start:end])
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError(f"Failed to parse LLM response as JSON: {e}")


def build_fallback_run_instructions(unit_components: list, order_map: dict) -> str:
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


# ---- ARM load strategy helpers (for testing path detection) ----

def detect_arm_load_strategy(path: str) -> str:
    """Return 'single' for .json files, 'directory' otherwise."""
    return "single" if path.endswith(".json") else "directory"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE = Path(__file__).parent / "sample_arm_template.json"


@pytest.fixture(scope="module")
def arm():
    with open(SAMPLE) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def arm_lookup(arm):
    return build_arm_lookup(arm)


@pytest.fixture(scope="module")
def sample_mapping_rows():
    """Simulated Component_Mapping rows (plain dicts)."""
    return [
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "Pipeline",
            "adf_component_name": "SalesDataPipeline",
            "adf_subtype": "",
            "parent_component": "",
            "databricks_equivalent": "Databricks Workflow (Job)",
            "complexity_score": "Medium",
            "adf_config_summary": "activities=2",
            "key_metrics": '{"activity_count": 2}',
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "Activity",
            "adf_component_name": "CopySqlToAdls",
            "adf_subtype": "Copy",
            "parent_component": "SalesDataPipeline",
            "databricks_equivalent": "Auto Loader / COPY INTO / Spark JDBC read",
            "complexity_score": "Medium",
            "adf_config_summary": "source=SqlServerSource",
            "key_metrics": '{"has_staging": true}',
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "Parameter",
            "adf_component_name": "startDate",
            "adf_subtype": "string",
            "parent_component": "SalesDataPipeline",
            "databricks_equivalent": "Databricks Workflow Job Parameter / dbutils.widgets",
            "complexity_score": "Low",
            "adf_config_summary": "param_type=string",
            "key_metrics": "{}",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "Variable",
            "adf_component_name": "rowCount",
            "adf_subtype": "String",
            "parent_component": "SalesDataPipeline",
            "databricks_equivalent": "dbutils.jobs.taskValues / Python variable",
            "complexity_score": "Low",
            "adf_config_summary": "var_type=String",
            "key_metrics": "{}",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "Dataset",
            "adf_component_name": "AzureSqlTransactions",
            "adf_subtype": "SqlServerTable",
            "parent_component": "",
            "databricks_equivalent": "Unity Catalog Table (ingested via JDBC/Lakeflow Connect)",
            "complexity_score": "Low",
            "adf_config_summary": "type=SqlServerTable",
            "key_metrics": '{"dataset_type": "SqlServerTable"}',
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "LinkedService",
            "adf_component_name": "AzureKeyVaultLinkedService",
            "adf_subtype": "AzureKeyVault",
            "parent_component": "",
            "databricks_equivalent": "Databricks Secret Scope backed by Azure Key Vault",
            "complexity_score": "Low",
            "adf_config_summary": "type=AzureKeyVault",
            "key_metrics": '{"service_type": "AzureKeyVault"}',
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "Trigger",
            "adf_component_name": "DailySchedule",
            "adf_subtype": "ScheduleTrigger",
            "parent_component": "",
            "databricks_equivalent": "Databricks Workflow Cron Schedule",
            "complexity_score": "Low",
            "adf_config_summary": "type=ScheduleTrigger",
            "key_metrics": '{"trigger_type": "ScheduleTrigger"}',
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "GlobalParameter",
            "adf_component_name": "environment",
            "adf_subtype": "string",
            "parent_component": "",
            "databricks_equivalent": "Databricks Workflow Job Parameter / Cluster Init Script / Notebook Widget",
            "complexity_score": "Low",
            "adf_config_summary": "param_type=string",
            "key_metrics": "{}",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "Credential",
            "adf_component_name": "ManagedIdentityCredential",
            "adf_subtype": "ManagedIdentity",
            "parent_component": "",
            "databricks_equivalent": "Unity Catalog Storage Credential (Managed Identity)",
            "complexity_score": "Low",
            "adf_config_summary": "cred_type=ManagedIdentity",
            "key_metrics": "{}",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "ManagedVirtualNetwork",
            "adf_component_name": "default",
            "adf_subtype": "Managed",
            "parent_component": "",
            "databricks_equivalent": "Databricks Workspace VNet Injection / Private Link",
            "complexity_score": "Medium",
            "adf_config_summary": "managed_vnet=true",
            "key_metrics": "{}",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "ManagedPrivateEndpoint",
            "adf_component_name": "AzureSqlPrivateEndpoint",
            "adf_subtype": "sqlServer",
            "parent_component": "",
            "databricks_equivalent": "Databricks Private Link / Private Endpoint / Serverless Network Connectivity",
            "complexity_score": "Medium",
            "adf_config_summary": "groupId=sqlServer",
            "key_metrics": "{}",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "DataFlow",
            "adf_component_name": "SalesTransformFlow",
            "adf_subtype": "MappingDataFlow",
            "parent_component": "",
            "databricks_equivalent": "Delta Live Tables (DLT) or PySpark Notebook",
            "complexity_score": "Medium",
            "adf_config_summary": "transformations=3",
            "key_metrics": '{"transformation_count": 3}',
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "IntegrationRuntime",
            "adf_component_name": "AutoResolveIntegrationRuntime",
            "adf_subtype": "Managed",
            "parent_component": "",
            "databricks_equivalent": "Serverless Compute or Job Cluster",
            "complexity_score": "Low",
            "adf_config_summary": "ir_type=Managed",
            "key_metrics": '{"ir_type": "Managed"}',
        },
        {
            "data_factory_name": "SalesDataFactory",
            "adf_component_type": "ChangeDataCapture",
            "adf_component_name": "SalesCDC",
            "adf_subtype": "Microbatch",
            "parent_component": "",
            "databricks_equivalent": "Delta Live Tables (DLT) with Change Data Feed / Lakeflow Connect CDC",
            "complexity_score": "Low",
            "adf_config_summary": "sources=1",
            "key_metrics": '{"source_count": 1}',
        },
    ]


@pytest.fixture(scope="module")
def sample_dep_rows():
    """Simulated Dependency_Analysis rows (plain dicts)."""
    return [
        {
            "data_factory_name": "SalesDataFactory",
            "component_name": "SalesDataPipeline",
            "migration_phase": "Phase 3",
            "migration_unit_id": "MU-001",
            "migration_unit_name": "Sales Application",
            "depends_on": "AzureSqlTransactions, SalesTransformFlow",
            "depended_on_by": "MasterOrchestrator, DailySchedule",
            "migration_unit_phase_plan": "Phase 1: LS:AzureKeyVaultLinkedService, IR:AutoResolveIntegrationRuntime | Phase 2: DS:AzureSqlTransactions | Phase 3: Pipeline:SalesDataPipeline",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "component_name": "AzureSqlTransactions",
            "migration_phase": "Phase 2",
            "migration_unit_id": "MU-001",
            "migration_unit_name": "Sales Application",
            "depends_on": "AzureSqlLinkedService",
            "depended_on_by": "SalesDataPipeline",
            "migration_unit_phase_plan": "Phase 1: LS:AzureKeyVaultLinkedService, IR:AutoResolveIntegrationRuntime | Phase 2: DS:AzureSqlTransactions | Phase 3: Pipeline:SalesDataPipeline",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "component_name": "AzureKeyVaultLinkedService",
            "migration_phase": "Phase 1",
            "migration_unit_id": "MU-001",
            "migration_unit_name": "Sales Application",
            "depends_on": "",
            "depended_on_by": "AzureSqlLinkedService, AdlsLinkedService",
            "migration_unit_phase_plan": "Phase 1: LS:AzureKeyVaultLinkedService, IR:AutoResolveIntegrationRuntime | Phase 2: DS:AzureSqlTransactions | Phase 3: Pipeline:SalesDataPipeline",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "component_name": "DailySchedule",
            "migration_phase": "Phase 4",
            "migration_unit_id": "MU-001",
            "migration_unit_name": "Sales Application",
            "depends_on": "SalesDataPipeline",
            "depended_on_by": "",
            "migration_unit_phase_plan": "Phase 1: LS:AzureKeyVaultLinkedService, IR:AutoResolveIntegrationRuntime | Phase 2: DS:AzureSqlTransactions | Phase 3: Pipeline:SalesDataPipeline",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "component_name": "SalesTransformFlow",
            "migration_phase": "Phase 2",
            "migration_unit_id": "MU-001",
            "migration_unit_name": "Sales Application",
            "depends_on": "AzureSqlTransactions",
            "depended_on_by": "SalesDataPipeline",
            "migration_unit_phase_plan": "Phase 1: LS:AzureKeyVaultLinkedService, IR:AutoResolveIntegrationRuntime | Phase 2: DS:AzureSqlTransactions | Phase 3: Pipeline:SalesDataPipeline",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "component_name": "AutoResolveIntegrationRuntime",
            "migration_phase": "Phase 1",
            "migration_unit_id": "MU-001",
            "migration_unit_name": "Sales Application",
            "depends_on": "",
            "depended_on_by": "SalesDataPipeline",
            "migration_unit_phase_plan": "Phase 1: LS:AzureKeyVaultLinkedService, IR:AutoResolveIntegrationRuntime | Phase 2: DS:AzureSqlTransactions | Phase 3: Pipeline:SalesDataPipeline",
        },
        {
            "data_factory_name": "SalesDataFactory",
            "component_name": "SalesCDC",
            "migration_phase": "Phase 1",
            "migration_unit_id": "MU-002",
            "migration_unit_name": "Shared Infrastructure",
            "depends_on": "",
            "depended_on_by": "",
            "migration_unit_phase_plan": "Phase 1: CDC:SalesCDC",
        },
    ]


@pytest.fixture(scope="module")
def merged_components(sample_mapping_rows, sample_dep_rows):
    return merge_component_data(sample_mapping_rows, sample_dep_rows)


# ---------------------------------------------------------------------------
# Test Classes
# ---------------------------------------------------------------------------


class TestComponentClassification:
    """Validates should_generate_code() for all component types."""

    def test_pipeline_generates_code(self):
        assert should_generate_code("Pipeline") is True

    def test_activity_copy_generates_code(self):
        assert should_generate_code("Activity") is True

    def test_dataset_generates_code(self):
        assert should_generate_code("Dataset") is True

    def test_linked_service_generates_code(self):
        assert should_generate_code("LinkedService") is True

    def test_dataflow_generates_code(self):
        assert should_generate_code("DataFlow") is True

    def test_trigger_generates_code(self):
        assert should_generate_code("Trigger") is True

    def test_integration_runtime_generates_code(self):
        assert should_generate_code("IntegrationRuntime") is True

    def test_change_data_capture_generates_code(self):
        assert should_generate_code("ChangeDataCapture") is True

    def test_parameter_skipped(self):
        assert should_generate_code("Parameter") is False

    def test_variable_skipped(self):
        assert should_generate_code("Variable") is False

    def test_global_parameter_skipped(self):
        assert should_generate_code("GlobalParameter") is False

    def test_credential_skipped(self):
        assert should_generate_code("Credential") is False

    def test_managed_vnet_skipped(self):
        assert should_generate_code("ManagedVirtualNetwork") is False

    def test_managed_private_endpoint_skipped(self):
        assert should_generate_code("ManagedPrivateEndpoint") is False


class TestPromptConstruction:
    """Validates build_user_prompt() JSON structure, truncation, deps, catalog refs."""

    def test_prompt_is_valid_json(self):
        comp = {"adf_component_type": "Pipeline", "adf_component_name": "TestPipe",
                "adf_subtype": "", "parent_component": "", "databricks_equivalent": "Workflow",
                "complexity_score": "Low", "adf_config_summary": "test", "key_metrics": "{}"}
        result = build_user_prompt(comp, "{}", "A", "B", "cat", "sch")
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_prompt_contains_component_fields(self):
        comp = {"adf_component_type": "Activity", "adf_component_name": "CopyData",
                "adf_subtype": "Copy", "parent_component": "Pipe1",
                "databricks_equivalent": "Auto Loader", "complexity_score": "Medium",
                "adf_config_summary": "source=Sql", "key_metrics": '{"x": 1}'}
        result = json.loads(build_user_prompt(comp, "{}", "", "", "cat", "sch"))
        assert result["adf_component_type"] == "Activity"
        assert result["adf_component_name"] == "CopyData"
        assert result["adf_subtype"] == "Copy"

    def test_prompt_includes_catalog_and_schema(self):
        comp = {"adf_component_type": "Pipeline", "adf_component_name": "P1"}
        result = json.loads(build_user_prompt(comp, "{}", "", "", "mycatalog", "myschema"))
        assert result["target_catalog"] == "mycatalog"
        assert result["target_schema"] == "myschema"

    def test_prompt_includes_dependencies(self):
        comp = {"adf_component_type": "Pipeline", "adf_component_name": "P1"}
        result = json.loads(build_user_prompt(comp, "{}", "DS1, DS2", "Trigger1", "c", "s"))
        assert result["depends_on"] == "DS1, DS2"
        assert result["depended_on_by"] == "Trigger1"

    def test_prompt_truncates_arm_fragment(self):
        comp = {"adf_component_type": "Pipeline", "adf_component_name": "P1"}
        long_arm = "x" * 5000
        result = json.loads(build_user_prompt(comp, long_arm, "", "", "c", "s"))
        assert len(result["arm_fragment"]) < 5000
        assert "truncated" in result["arm_fragment"]

    def test_prompt_does_not_truncate_short_arm(self):
        comp = {"adf_component_type": "Pipeline", "adf_component_name": "P1"}
        short_arm = '{"name": "test"}'
        result = json.loads(build_user_prompt(comp, short_arm, "", "", "c", "s"))
        assert result["arm_fragment"] == short_arm

    def test_prompt_has_all_expected_keys(self):
        comp = {"adf_component_type": "Pipeline", "adf_component_name": "P1"}
        result = json.loads(build_user_prompt(comp, "{}", "", "", "c", "s"))
        expected_keys = {"adf_component_type", "adf_component_name", "adf_subtype",
                        "parent_component", "databricks_equivalent", "complexity_score",
                        "adf_config_summary", "key_metrics", "arm_fragment",
                        "depends_on", "depended_on_by", "target_catalog", "target_schema"}
        assert expected_keys == set(result.keys())

    def test_prompt_handles_empty_component(self):
        comp = {}
        result = json.loads(build_user_prompt(comp, "", "", "", "c", "s"))
        assert result["adf_component_type"] == ""
        assert result["adf_component_name"] == ""


class TestResponseParsing:
    """Validates parse_llm_response() — clean JSON, markdown fences, preamble, nested braces, invalid."""

    def test_clean_json(self):
        text = '{"code": "print(1)", "code_language": "python", "run_instructions": "Run it", "notes": ""}'
        result = parse_llm_response(text)
        assert result["code"] == "print(1)"
        assert result["code_language"] == "python"

    def test_markdown_fences(self):
        text = '```json\n{"code": "SELECT 1", "code_language": "sql", "run_instructions": "Execute", "notes": ""}\n```'
        result = parse_llm_response(text)
        assert result["code"] == "SELECT 1"

    def test_preamble_text(self):
        text = 'Here is the result:\n\n{"code": "x=1", "code_language": "python", "run_instructions": "Deploy", "notes": "none"}'
        result = parse_llm_response(text)
        assert result["code"] == "x=1"

    def test_nested_braces_in_code(self):
        inner = {"code": 'def f():\n    d = {"a": 1}\n    return d',
                 "code_language": "python", "run_instructions": "Run", "notes": ""}
        text = json.dumps(inner)
        result = parse_llm_response(text)
        assert "def f()" in result["code"]

    def test_invalid_json_raises(self):
        with pytest.raises(ValueError):
            parse_llm_response("This is not JSON at all")


class TestExecutionOrder:
    """Validates compute_execution_order() — phase plan parsing."""

    def test_multi_phase_plan(self):
        plan = "Phase 1: LS:KV, IR:Auto | Phase 2: DS:Sales | Phase 3: Pipeline:Main"
        order = compute_execution_order(plan)
        assert order["KV"] == 1
        assert order["Auto"] == 2
        assert order["Sales"] == 3
        assert order["Main"] == 4

    def test_single_phase(self):
        plan = "Phase 1: LS:KV"
        order = compute_execution_order(plan)
        assert order["KV"] == 1
        assert len(order) == 1

    def test_empty_plan(self):
        assert compute_execution_order("") == {}

    def test_preserves_order_across_phases(self):
        plan = "Phase 1: LS:A, LS:B | Phase 2: DS:C | Phase 3: Pipeline:D, Pipeline:E"
        order = compute_execution_order(plan)
        assert order["A"] < order["B"] < order["C"] < order["D"] < order["E"]


class TestMergeLogic:
    """Validates merge_component_data() — matching, missing deps, activity inheritance."""

    def test_pipeline_has_dep_data(self, merged_components):
        pipe = next(c for c in merged_components if c["adf_component_name"] == "SalesDataPipeline")
        assert pipe["migration_phase"] == "Phase 3"
        assert pipe["migration_unit_id"] == "MU-001"

    def test_activity_inherits_parent_phase(self, merged_components):
        """CopySqlToAdls has parent=SalesDataPipeline, should inherit its phase."""
        copy = next(c for c in merged_components if c["adf_component_name"] == "CopySqlToAdls")
        assert copy["migration_phase"] == "Phase 3"
        assert copy["migration_unit_id"] == "MU-001"

    def test_component_without_dep_data_gets_empty(self, merged_components):
        """GlobalParameter 'environment' has no dep match and no parent → empty."""
        gp = next(c for c in merged_components if c["adf_component_name"] == "environment")
        assert gp["migration_phase"] == ""

    def test_all_mapping_fields_preserved(self, merged_components):
        pipe = next(c for c in merged_components if c["adf_component_name"] == "SalesDataPipeline")
        assert pipe["databricks_equivalent"] == "Databricks Workflow (Job)"
        assert pipe["complexity_score"] == "Medium"
        assert pipe["adf_config_summary"] == "activities=2"


class TestArmLookup:
    """Validates build_arm_lookup() — finds components, handles missing."""

    def test_finds_pipeline(self, arm_lookup):
        assert "SalesDataPipeline" in arm_lookup

    def test_finds_linked_service(self, arm_lookup):
        assert "AzureKeyVaultLinkedService" in arm_lookup

    def test_finds_dataset(self, arm_lookup):
        assert "AzureSqlTransactions" in arm_lookup

    def test_finds_trigger(self, arm_lookup):
        assert "DailySchedule" in arm_lookup

    def test_missing_component_not_in_lookup(self, arm_lookup):
        assert "NonExistentComponent" not in arm_lookup


class TestArmLoadStrategy:
    """Path detection: .json → single file, directory → glob merge, merged resources."""

    def test_json_file_detected_as_single(self):
        assert detect_arm_load_strategy("/path/to/template.json") == "single"

    def test_directory_detected_as_directory(self):
        assert detect_arm_load_strategy("/path/to/templates/") == "directory"

    def test_directory_without_trailing_slash(self):
        assert detect_arm_load_strategy("/path/to/templates") == "directory"


class TestTokenBudget:
    """Validates get_max_tokens() — Low/Medium/High/Unknown."""

    def test_low_complexity(self):
        assert get_max_tokens("Low") == 1000

    def test_medium_complexity(self):
        assert get_max_tokens("Medium") == 2000

    def test_high_complexity(self):
        assert get_max_tokens("High") == 3000

    def test_unknown_complexity_defaults(self):
        assert get_max_tokens("Unknown") == 2000


class TestSkipReasons:
    """Validates get_skip_reason() — meaningful text for each type."""

    def test_parameter_reason(self):
        reason = get_skip_reason("Parameter")
        assert "Workflow job parameters" in reason

    def test_variable_reason(self):
        reason = get_skip_reason("Variable")
        assert "taskValues" in reason

    def test_credential_reason(self):
        reason = get_skip_reason("Credential")
        assert "Secret Scope" in reason

    def test_unknown_type_reason(self):
        reason = get_skip_reason("SomeUnknownType")
        assert "SomeUnknownType" in reason


class TestFallbackRunInstructions:
    """Validates build_fallback_run_instructions() — numbered plan, empty."""

    def test_numbered_plan(self):
        components = [
            {"adf_component_type": "LinkedService", "adf_component_name": "KV",
             "databricks_equivalent": "Secret Scope"},
            {"adf_component_type": "Pipeline", "adf_component_name": "Main",
             "databricks_equivalent": "Workflow"},
        ]
        order_map = {"KV": 1, "Main": 2}
        result = build_fallback_run_instructions(components, order_map)
        lines = result.strip().split("\n")
        assert len(lines) == 2
        assert lines[0].startswith("1.")
        assert lines[1].startswith("2.")
        assert "KV" in lines[0]
        assert "Main" in lines[1]

    def test_empty_components(self):
        assert build_fallback_run_instructions([], {}) == ""


class TestPhaseFiltering:
    """Validates apply_phase_filter() — 'all', specific, case-insensitive."""

    def test_all_returns_everything(self, merged_components):
        result = apply_phase_filter(merged_components, "all")
        assert len(result) == len(merged_components)

    def test_specific_phase(self, merged_components):
        result = apply_phase_filter(merged_components, "Phase 3")
        # SalesDataPipeline + CopySqlToAdls (inherited) + rowCount (inherited) + startDate (inherited)
        for c in result:
            assert c["migration_phase"] == "Phase 3"

    def test_case_insensitive(self, merged_components):
        result1 = apply_phase_filter(merged_components, "phase 1")
        result2 = apply_phase_filter(merged_components, "Phase 1")
        assert len(result1) == len(result2)


class TestOutputRowSchema:
    """All 26 columns present for success/failed/skipped rows."""

    EXPECTED_COLUMNS = {
        "conversion_timestamp", "data_factory_name", "adf_component_type",
        "adf_component_name", "adf_subtype", "parent_component",
        "databricks_equivalent", "migration_phase", "migration_unit_id",
        "migration_unit_name", "complexity_score", "migration_status",
        "status_description", "generated_code", "code_language",
        "run_instructions", "execution_order", "depends_on", "depended_on_by",
        "llm_endpoint_used", "llm_prompt_tokens", "llm_completion_tokens",
        "generation_duration_seconds", "retry_count", "adf_config_summary",
        "key_metrics",
    }

    def test_success_row_has_all_columns(self):
        comp = {"adf_component_type": "Pipeline", "adf_component_name": "P1",
                "data_factory_name": "F1", "adf_subtype": "", "parent_component": "",
                "databricks_equivalent": "Workflow", "migration_phase": "Phase 1",
                "migration_unit_id": "MU-001", "migration_unit_name": "Test",
                "complexity_score": "Low", "depends_on": "", "depended_on_by": "",
                "adf_config_summary": "test", "key_metrics": "{}"}
        llm_result = {"migration_status": "success", "status_description": "ok",
                      "generated_code": "print(1)", "code_language": "python",
                      "run_instructions": "Deploy", "generation_duration_seconds": 1.5,
                      "retry_count": 0, "llm_prompt_tokens": 100, "llm_completion_tokens": 50}
        row = build_output_row(comp, llm_result, "2026-01-01", "llama-70b", 1)
        assert set(row.keys()) == self.EXPECTED_COLUMNS
        assert row["migration_status"] == "success"

    def test_failed_row_has_all_columns(self):
        comp = {"adf_component_type": "Pipeline", "adf_component_name": "P1",
                "data_factory_name": "F1", "adf_subtype": "", "parent_component": "",
                "databricks_equivalent": "Workflow", "migration_phase": "Phase 1",
                "migration_unit_id": "MU-001", "migration_unit_name": "Test",
                "complexity_score": "High", "depends_on": "", "depended_on_by": "",
                "adf_config_summary": "test", "key_metrics": "{}"}
        llm_result = {"migration_status": "failed", "status_description": "timeout",
                      "generated_code": "", "code_language": "none",
                      "run_instructions": "", "generation_duration_seconds": 30.0,
                      "retry_count": 2, "llm_prompt_tokens": 0, "llm_completion_tokens": 0}
        row = build_output_row(comp, llm_result, "2026-01-01", "llama-70b", 1)
        assert set(row.keys()) == self.EXPECTED_COLUMNS
        assert row["migration_status"] == "failed"

    def test_skipped_row_has_all_columns(self):
        comp = {"adf_component_type": "Parameter", "adf_component_name": "startDate",
                "data_factory_name": "F1", "adf_subtype": "string", "parent_component": "P1",
                "databricks_equivalent": "Widget", "migration_phase": "Phase 1",
                "migration_unit_id": "MU-001", "migration_unit_name": "Test",
                "complexity_score": "Low", "depends_on": "", "depended_on_by": "",
                "adf_config_summary": "test", "key_metrics": "{}"}
        row = build_skipped_row(comp, "2026-01-01", "llama-70b", 1)
        assert set(row.keys()) == self.EXPECTED_COLUMNS
        assert row["migration_status"] == "skipped"
        assert row["generated_code"] == ""
        assert row["llm_prompt_tokens"] == 0

    def test_skipped_row_has_meaningful_description(self):
        comp = {"adf_component_type": "Parameter", "adf_component_name": "startDate",
                "data_factory_name": "F1", "adf_subtype": "string", "parent_component": "P1",
                "databricks_equivalent": "Widget", "migration_phase": "Phase 1",
                "migration_unit_id": "MU-001", "migration_unit_name": "Test",
                "complexity_score": "Low", "depends_on": "", "depended_on_by": "",
                "adf_config_summary": "test", "key_metrics": "{}"}
        row = build_skipped_row(comp, "2026-01-01", "llama-70b", 1)
        assert row["status_description"] != ""
        assert "Workflow job parameters" in row["status_description"]
