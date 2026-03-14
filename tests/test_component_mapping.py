"""
Tests for the ADF → Databricks Component Mapping logic.

Validates component expansion (including Parameters/Variables and all
additional ADF component types), the rule-based mapping table, and
complexity scoring against the sample ARM template.

Run:  pytest tests/test_component_mapping.py -v
"""

import json
import re
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

def _name(resource):
    raw = resource.get("name") or resource.get("Name") or "unknown"
    m = re.search(r"'([^']+)'\s*\)\s*\]$", raw)
    if m:
        path = m.group(1).lstrip("/")
        return path.rsplit("/", 1)[-1] if "/" in path else path
    return raw.rsplit("/", 1)[-1] if "/" in raw else raw

def _props(r):
    return r.get("properties") or r.get("Properties") or {}

def parse_arm(template):
    factory, comps = "", []
    factory_props = {}
    for r in template.get("resources", []):
        rt = r.get("type", "")
        if rt == "Microsoft.DataFactory/factories":
            factory = _name(r)
            factory_props = _props(r)
            continue
        friendly = TYPE_MAP.get(rt)
        if friendly:
            comps.append({"type": friendly, "name": _name(r), "properties": _props(r)})
    # Extract global parameters from factory resource
    gp = factory_props.get("globalParameters", {})
    for gp_name, gp_def in gp.items():
        comps.append({
            "type": "GlobalParameter",
            "name": gp_name,
            "properties": gp_def if isinstance(gp_def, dict) else {"type": "string", "value": gp_def},
        })
    return factory, comps


def expand_components(components):
    all_items = []
    for comp in components:
        ctype, name, props = comp["type"], comp["name"], comp["properties"]
        subtype = ""
        if ctype == "LinkedService":  subtype = props.get("type", "")
        elif ctype == "Dataset":      subtype = props.get("type", "")
        elif ctype == "Trigger":      subtype = props.get("type", "")
        elif ctype == "DataFlow":     subtype = props.get("type", "MappingDataFlow")
        elif ctype == "IntegrationRuntime": subtype = props.get("type", "Managed")
        elif ctype == "ChangeDataCapture":
            policy = props.get("policy", {})
            subtype = policy.get("mode", "Microbatch") if isinstance(policy, dict) else "Microbatch"
        elif ctype == "Credential":   subtype = props.get("type", "")
        elif ctype == "GlobalParameter": subtype = props.get("type", "string")
        elif ctype == "ManagedVirtualNetwork": subtype = "Managed"
        elif ctype == "ManagedPrivateEndpoint": subtype = props.get("groupId", "")
        elif ctype == "PrivateEndpointConnection":
            subtype = props.get("privateLinkServiceConnectionState", {}).get("status", "") if isinstance(props.get("privateLinkServiceConnectionState"), dict) else ""
        all_items.append({"type": ctype, "name": name, "parent": "", "subtype": subtype, "properties": props})
        if ctype == "Pipeline":
            for act in props.get("activities", []):
                at = act.get("type", "")
                all_items.append({"type": "Activity", "name": act.get("name", "unnamed"),
                                  "parent": name, "subtype": at, "properties": act})
                tp = act.get("typeProperties", {})
                inner = []
                if at == "ForEach":      inner = tp.get("activities", [])
                elif at == "IfCondition": inner = tp.get("ifTrueActivities", []) + tp.get("ifFalseActivities", [])
                elif at == "Switch":
                    for case in tp.get("cases", []):
                        inner.extend(case.get("activities", []))
                    inner.extend(tp.get("defaultActivities", []))
                elif at == "Until":      inner = tp.get("activities", [])
                for child in inner:
                    all_items.append({"type": "Activity", "name": child.get("name", "unnamed"),
                                      "parent": f"{name}/{act.get('name','')}", "subtype": child.get("type", ""),
                                      "properties": child})
            for pname, pdef in props.get("parameters", {}).items():
                ptype = pdef.get("type", "string") if isinstance(pdef, dict) else "string"
                all_items.append({"type": "Parameter", "name": pname, "parent": name,
                                  "subtype": ptype, "properties": pdef if isinstance(pdef, dict) else {"type": ptype}})
            for vname, vdef in props.get("variables", {}).items():
                vtype = vdef.get("type", "String") if isinstance(vdef, dict) else "String"
                all_items.append({"type": "Variable", "name": vname, "parent": name,
                                  "subtype": vtype, "properties": vdef if isinstance(vdef, dict) else {"type": vtype}})
    return all_items


def _count_expressions(obj, depth=0):
    if depth > 20: return 0
    if isinstance(obj, str):
        return len(re.findall(r"@\{|@pipeline\(\)|@activity\(\)|@dataset\(\)|@concat\(|@utcNow\(", obj))
    if isinstance(obj, dict): return sum(_count_expressions(v, depth+1) for v in obj.values())
    if isinstance(obj, list): return sum(_count_expressions(v, depth+1) for v in obj)
    return 0


def extract_metrics(item):
    """Replicate extract_metrics from the notebook for testing."""
    props = item["properties"]
    ctype = item["type"]
    sub = item.get("subtype", "")
    m = {}

    if ctype == "Pipeline":
        activities = props.get("activities", [])
        act_types = [a.get("type", "") for a in activities]
        m["activity_count"] = len(activities)
        m["activity_types"] = sorted(set(act_types))
        m["parameter_count"] = len(props.get("parameters", {}))
        m["variable_count"] = len(props.get("variables", {}))
        m["has_foreach"] = "ForEach" in act_types
        m["has_until"] = "Until" in act_types
        m["has_switch"] = "Switch" in act_types
        m["has_ifcondition"] = "IfCondition" in act_types
        m["has_execute_pipeline"] = "ExecutePipeline" in act_types
        m["has_dataflow"] = "ExecuteDataFlow" in act_types
        m["has_web_activity"] = "WebActivity" in act_types
        m["expression_count"] = _count_expressions(props)
    elif ctype == "Activity":
        m["activity_type"] = sub
        m["has_retry"] = props.get("policy", {}).get("retry", 0) > 0
        m["expression_count"] = _count_expressions(props.get("typeProperties", {}))
        tp = props.get("typeProperties", {})
        if sub == "Copy":
            m["has_staging"] = tp.get("enableStaging", False)
            m["has_column_mapping"] = "translator" in tp or "mappings" in tp
            m["source_type"] = tp.get("source", {}).get("type", "")
            m["sink_type"] = tp.get("sink", {}).get("type", "")
        elif sub == "ForEach":
            m["is_sequential"] = tp.get("isSequential", False)
            m["inner_activity_count"] = len(tp.get("activities", []))
        elif sub == "ExecutePipeline":
            m["child_pipeline"] = tp.get("pipeline", {}).get("referenceName", "")
    elif ctype == "DataFlow":
        tp = props.get("typeProperties", {})
        m["source_count"] = len(tp.get("sources", []))
        m["sink_count"] = len(tp.get("sinks", []))
        m["transformation_count"] = len(tp.get("transformations", []))
        script = tp.get("script", "")
        m["has_join"] = "join(" in script.lower()
        m["has_aggregate"] = "aggregate(" in script.lower()
        m["has_pivot"] = "pivot(" in script.lower()
    elif ctype == "Dataset":
        m["dataset_type"] = props.get("type", "")
        m["linked_service"] = props.get("linkedServiceName", {}).get("referenceName", "")
        m["parameter_count"] = len(props.get("parameters", {}))
        m["has_schema"] = bool(props.get("schema"))
    elif ctype == "LinkedService":
        m["service_type"] = props.get("type", "")
        tp = props.get("typeProperties", {})
        m["uses_key_vault"] = "AzureKeyVaultSecret" in json.dumps(tp)
        m["has_parameters"] = bool(props.get("parameters"))
    elif ctype == "Trigger":
        m["trigger_type"] = props.get("type", "")
        m["pipeline_count"] = len(props.get("pipelines", []))
        tp = props.get("typeProperties", {})
        m["has_retry_policy"] = "retryPolicy" in tp
    elif ctype == "IntegrationRuntime":
        m["ir_type"] = props.get("type", "")
    return m


def rule_based_complexity(item):
    props, ctype, sub = item["properties"], item["type"], item["subtype"]
    score, reasons = 0, []
    if ctype == "Pipeline":
        acts = props.get("activities", [])
        act_types = {a.get("type", "") for a in acts}
        n = len(acts)
        if n > 10:  score += 3
        elif n > 4: score += 1
        for ctrl in ("ForEach", "Until", "Switch"):
            if ctrl in act_types: score += 2; reasons.append(f"has {ctrl}")
        if "IfCondition" in act_types:    score += 1
        if "ExecutePipeline" in act_types: score += 2
        if "ExecuteDataFlow" in act_types: score += 1
    elif ctype == "Activity":
        if sub in ("ForEach", "Until", "Switch"):  score += 2
        if sub == "ExecutePipeline":                score += 2
        if sub == "Custom":                         score += 3
        if sub == "ExecuteSSISPackage":             score += 4
        if sub in ("AzureMLExecutePipeline", "AzureMLBatchExecution", "AzureMLUpdateResource"):
            score += 3
        if sub == "USql":                           score += 3
        if sub in ("HDInsightHive", "HDInsightPig", "HDInsightMapReduce", "HDInsightStreaming"):
            score += 2
        if sub == "WebHook":                        score += 1
        tp = props.get("typeProperties", {})
        if sub == "Copy":
            if tp.get("enableStaging"): score += 1
            if "translator" in tp:      score += 1
    elif ctype == "DataFlow":
        tp = props.get("typeProperties", {})
        tc = len(tp.get("transformations", []))
        script = tp.get("script", "")
        if tc >= 5:   score += 3
        elif tc >= 2: score += 1
        if "join(" in script.lower():  score += 1
        if "pivot(" in script.lower(): score += 2
    elif ctype == "Trigger":
        if sub == "TumblingWindowTrigger": score += 2
    elif ctype == "IntegrationRuntime":
        if sub == "SelfHosted": score += 3
    elif ctype == "ChangeDataCapture":
        source_count = len(props.get("typeProperties", {}).get("sourceConnectionsInfo", []))
        if source_count > 2: score += 2
        policy = props.get("policy", {})
        if isinstance(policy, dict) and policy.get("mode") == "Microbatch":
            score += 1
    elif ctype == "Credential":
        if sub == "ServicePrincipal": score += 1
    elif ctype == "GlobalParameter":
        gp_type = props.get("type", "string") if isinstance(props, dict) else "string"
        if gp_type in ("Array", "Object", "array", "object"): score += 1
    elif ctype == "ManagedVirtualNetwork":
        score += 2
    elif ctype == "ManagedPrivateEndpoint":
        score += 2
    elif ctype == "PrivateEndpointConnection":
        score += 2
    if score >= 5:   return "High", "; ".join(reasons)
    elif score >= 2: return "Medium", "; ".join(reasons)
    return "Low", "Standard component"


# ---- Fixtures ----

SAMPLE = Path(__file__).parent / "sample_arm_template.json"

@pytest.fixture(scope="module")
def arm():
    with open(SAMPLE) as f:
        return json.load(f)

@pytest.fixture(scope="module")
def parsed(arm):
    return parse_arm(arm)

@pytest.fixture(scope="module")
def expanded(parsed):
    _, comps = parsed
    return expand_components(comps)


# ---- Tests ----

class TestExpansion:

    def test_pipelines_present(self, expanded):
        pipelines = [i for i in expanded if i["type"] == "Pipeline"]
        assert len(pipelines) == 2

    def test_activities_extracted(self, expanded):
        activities = [i for i in expanded if i["type"] == "Activity"]
        # SalesDataPipeline: CopySqlToAdls, ExecuteSalesTransformFlow (2)
        # MasterOrchestrator: ProcessRegionsPipeline(ForEach), CheckFullLoadCondition(IfCondition) (2)
        # Inner: ExecuteSalesDataPipeline (from ForEach), SendSuccessNotification + SendIncrementalNotification (from IfCondition)
        assert len(activities) >= 7

    def test_parameters_extracted(self, expanded):
        params = [i for i in expanded if i["type"] == "Parameter"]
        param_names = {p["name"] for p in params}
        assert "startDate" in param_names
        assert "endDate" in param_names
        assert "regions" in param_names
        assert "isFullLoad" in param_names
        assert "notificationUrl" in param_names

    def test_variables_extracted(self, expanded):
        variables = [i for i in expanded if i["type"] == "Variable"]
        assert any(v["name"] == "rowCount" for v in variables)

    def test_datasets_present(self, expanded):
        datasets = [i for i in expanded if i["type"] == "Dataset"]
        assert len(datasets) == 2
        names = {d["name"] for d in datasets}
        assert "AzureSqlTransactions" in names
        assert "AdlsTransactionsParquet" in names

    def test_linked_services_with_subtypes(self, expanded):
        ls = [i for i in expanded if i["type"] == "LinkedService"]
        subtypes = {l["subtype"] for l in ls}
        assert "AzureSqlDatabase" in subtypes
        assert "AzureBlobFS" in subtypes
        assert "AzureKeyVault" in subtypes

    def test_triggers_with_subtypes(self, expanded):
        triggers = [i for i in expanded if i["type"] == "Trigger"]
        subtypes = {t["subtype"] for t in triggers}
        assert "ScheduleTrigger" in subtypes
        assert "TumblingWindowTrigger" in subtypes

    def test_dataflow_present(self, expanded):
        dfs = [i for i in expanded if i["type"] == "DataFlow"]
        assert len(dfs) == 1
        assert dfs[0]["name"] == "SalesTransformFlow"

    def test_ir_present(self, expanded):
        irs = [i for i in expanded if i["type"] == "IntegrationRuntime"]
        assert len(irs) == 1
        assert irs[0]["subtype"] == "Managed"

    # --- New component types ---

    def test_cdc_present(self, expanded):
        cdcs = [i for i in expanded if i["type"] == "ChangeDataCapture"]
        assert len(cdcs) == 1
        assert cdcs[0]["name"] == "SalesCDC"
        assert cdcs[0]["subtype"] == "Microbatch"

    def test_credential_present(self, expanded):
        creds = [i for i in expanded if i["type"] == "Credential"]
        assert len(creds) == 1
        assert creds[0]["name"] == "ManagedIdentityCredential"
        assert creds[0]["subtype"] == "ManagedIdentity"

    def test_global_parameters_from_factory(self, expanded):
        gps = [i for i in expanded if i["type"] == "GlobalParameter"]
        gp_names = {g["name"] for g in gps}
        assert "environment" in gp_names
        assert "maxRetryCount" in gp_names

    def test_managed_vnet_present(self, expanded):
        vnets = [i for i in expanded if i["type"] == "ManagedVirtualNetwork"]
        assert len(vnets) == 1

    def test_managed_private_endpoint_present(self, expanded):
        mpes = [i for i in expanded if i["type"] == "ManagedPrivateEndpoint"]
        assert len(mpes) == 1
        assert mpes[0]["name"] == "AzureSqlPrivateEndpoint"
        assert mpes[0]["subtype"] == "sqlServer"

    def test_all_component_types_covered(self, expanded):
        types = {i["type"] for i in expanded}
        expected = {
            "Pipeline", "Activity", "Dataset", "LinkedService",
            "DataFlow", "Trigger", "IntegrationRuntime", "Parameter", "Variable",
            "ChangeDataCapture", "Credential", "GlobalParameter",
            "ManagedVirtualNetwork", "ManagedPrivateEndpoint",
        }
        assert expected.issubset(types), f"Missing: {expected - types}"

    def test_parent_set_for_children(self, expanded):
        for item in expanded:
            if item["type"] in ("Activity", "Parameter", "Variable"):
                assert item["parent"] != "", f"{item['type']} {item['name']} has no parent"


class TestComplexityScoring:

    def test_master_orchestrator_is_medium_or_high(self, expanded):
        """ForEach(2) + IfCondition(1) = 3 → Medium.
        ExecutePipeline is nested inside ForEach so not counted at top level."""
        orch = next(i for i in expanded if i["name"] == "MasterOrchestrator" and i["type"] == "Pipeline")
        score, _ = rule_based_complexity(orch)
        assert score in ("Medium", "High")

    def test_sales_pipeline_not_high(self, expanded):
        """2 activities (Copy + ExecuteDataFlow). ExecuteDataFlow adds +1 → Low or Medium."""
        sales = next(i for i in expanded if i["name"] == "SalesDataPipeline" and i["type"] == "Pipeline")
        score, _ = rule_based_complexity(sales)
        assert score in ("Low", "Medium")

    def test_copy_with_staging_is_medium(self, expanded):
        copy = next(i for i in expanded if i["name"] == "CopySqlToAdls" and i["type"] == "Activity")
        score, _ = rule_based_complexity(copy)
        assert score == "Medium"

    def test_foreach_activity_is_medium(self, expanded):
        fe = next(i for i in expanded if i["subtype"] == "ForEach" and i["type"] == "Activity")
        score, _ = rule_based_complexity(fe)
        assert score in ("Medium", "High")

    def test_execute_pipeline_activity_is_medium(self, expanded):
        ep = next(i for i in expanded if i["subtype"] == "ExecutePipeline" and i["type"] == "Activity")
        score, _ = rule_based_complexity(ep)
        assert score in ("Medium", "High")

    def test_simple_dataset_is_low(self, expanded):
        ds = next(i for i in expanded if i["name"] == "AzureSqlTransactions" and i["type"] == "Dataset")
        score, _ = rule_based_complexity(ds)
        assert score == "Low"

    def test_tumbling_window_trigger_is_medium(self, expanded):
        tw = next(i for i in expanded if i["subtype"] == "TumblingWindowTrigger")
        score, _ = rule_based_complexity(tw)
        assert score == "Medium"

    def test_managed_ir_is_low(self, expanded):
        ir = next(i for i in expanded if i["type"] == "IntegrationRuntime")
        score, _ = rule_based_complexity(ir)
        assert score == "Low"

    def test_simple_parameter_is_low(self, expanded):
        param = next(i for i in expanded if i["type"] == "Parameter" and i["name"] == "startDate")
        score, _ = rule_based_complexity(param)
        assert score == "Low"

    def test_variable_is_low(self, expanded):
        var = next(i for i in expanded if i["type"] == "Variable")
        score, _ = rule_based_complexity(var)
        assert score == "Low"

    def test_dataflow_with_join_is_medium(self, expanded):
        df = next(i for i in expanded if i["type"] == "DataFlow")
        score, _ = rule_based_complexity(df)
        assert score == "Medium"

    # --- New component type complexity ---

    def test_cdc_is_low(self, expanded):
        """1 source connection + Microbatch mode = 1 → Low."""
        cdc = next(i for i in expanded if i["type"] == "ChangeDataCapture")
        score, _ = rule_based_complexity(cdc)
        assert score == "Low"

    def test_managed_identity_credential_is_low(self, expanded):
        cred = next(i for i in expanded if i["type"] == "Credential")
        score, _ = rule_based_complexity(cred)
        assert score == "Low"

    def test_global_parameter_simple_is_low(self, expanded):
        gp = next(i for i in expanded if i["type"] == "GlobalParameter" and i["name"] == "environment")
        score, _ = rule_based_complexity(gp)
        assert score == "Low"

    def test_managed_vnet_is_medium(self, expanded):
        vnet = next(i for i in expanded if i["type"] == "ManagedVirtualNetwork")
        score, _ = rule_based_complexity(vnet)
        assert score == "Medium"

    def test_managed_private_endpoint_is_medium(self, expanded):
        mpe = next(i for i in expanded if i["type"] == "ManagedPrivateEndpoint")
        score, _ = rule_based_complexity(mpe)
        assert score == "Medium"


class TestSyntheticActivityComplexity:
    """Test complexity scoring for activity subtypes not in the sample template."""

    def test_ssis_activity_is_medium_or_high(self):
        """ExecuteSSISPackage scores 4 → Medium (High if combined with other factors)."""
        item = {"type": "Activity", "name": "RunSSIS", "parent": "P1",
                "subtype": "ExecuteSSISPackage", "properties": {"typeProperties": {}}}
        score, _ = rule_based_complexity(item)
        assert score in ("Medium", "High")

    def test_azure_ml_activity_is_medium_or_high(self):
        """AzureMLExecutePipeline scores 3 → Medium (High if combined with other factors)."""
        item = {"type": "Activity", "name": "RunML", "parent": "P1",
                "subtype": "AzureMLExecutePipeline", "properties": {"typeProperties": {}}}
        score, _ = rule_based_complexity(item)
        assert score in ("Medium", "High")

    def test_usql_activity_is_medium_or_high(self):
        """USql scores 3 → Medium (High if combined with other factors)."""
        item = {"type": "Activity", "name": "RunUSQL", "parent": "P1",
                "subtype": "USql", "properties": {"typeProperties": {}}}
        score, _ = rule_based_complexity(item)
        assert score in ("Medium", "High")

    def test_hdinsight_hive_is_medium(self):
        item = {"type": "Activity", "name": "RunHive", "parent": "P1",
                "subtype": "HDInsightHive", "properties": {"typeProperties": {}}}
        score, _ = rule_based_complexity(item)
        assert score == "Medium"

    def test_filter_is_low(self):
        item = {"type": "Activity", "name": "FilterItems", "parent": "P1",
                "subtype": "Filter", "properties": {"typeProperties": {}}}
        score, _ = rule_based_complexity(item)
        assert score == "Low"

    def test_fail_is_low(self):
        item = {"type": "Activity", "name": "FailPipeline", "parent": "P1",
                "subtype": "Fail", "properties": {"typeProperties": {}}}
        score, _ = rule_based_complexity(item)
        assert score == "Low"

    def test_webhook_is_low(self):
        item = {"type": "Activity", "name": "CallWebhook", "parent": "P1",
                "subtype": "WebHook", "properties": {"typeProperties": {}}}
        score, _ = rule_based_complexity(item)
        assert score == "Low"


class TestKeyMetrics:
    """Test the extract_metrics function (merged from Inventory Analyzer)."""

    def test_pipeline_metrics(self, expanded):
        sales = next(i for i in expanded if i["name"] == "SalesDataPipeline" and i["type"] == "Pipeline")
        m = extract_metrics(sales)
        assert m["activity_count"] == 2
        assert m["parameter_count"] == 2
        assert m["variable_count"] == 1
        assert m["has_dataflow"] is True
        assert m["has_foreach"] is False
        assert "activity_types" in m
        assert isinstance(m["activity_types"], list)

    def test_orchestrator_metrics(self, expanded):
        orch = next(i for i in expanded if i["name"] == "MasterOrchestrator" and i["type"] == "Pipeline")
        m = extract_metrics(orch)
        assert m["has_foreach"] is True
        assert m["has_ifcondition"] is True
        assert m["parameter_count"] == 3

    def test_dataflow_metrics(self, expanded):
        df = next(i for i in expanded if i["type"] == "DataFlow")
        m = extract_metrics(df)
        assert m["transformation_count"] == 3
        assert m["has_join"] is True
        assert m["has_aggregate"] is True
        assert m["source_count"] == 1
        assert m["sink_count"] == 1

    def test_activity_metrics(self, expanded):
        copy_act = next(i for i in expanded if i["name"] == "CopySqlToAdls" and i["type"] == "Activity")
        m = extract_metrics(copy_act)
        assert m["activity_type"] == "Copy"
        assert m["has_staging"] is True
        assert "source_type" in m
        assert "sink_type" in m

    def test_dataset_metrics(self, expanded):
        ds = next(i for i in expanded if i["name"] == "AzureSqlTransactions" and i["type"] == "Dataset")
        m = extract_metrics(ds)
        assert m["dataset_type"] == "SqlServerTable"
        assert m["linked_service"] == "AzureSqlLinkedService"

    def test_linked_service_metrics(self, expanded):
        ls = next(i for i in expanded if i["name"] == "AzureKeyVaultLinkedService" and i["type"] == "LinkedService")
        m = extract_metrics(ls)
        assert m["service_type"] == "AzureKeyVault"

    def test_trigger_metrics(self, expanded):
        tw = next(i for i in expanded if i["subtype"] == "TumblingWindowTrigger")
        m = extract_metrics(tw)
        assert m["trigger_type"] == "TumblingWindowTrigger"
        assert m["has_retry_policy"] is True

    def test_ir_metrics(self, expanded):
        ir = next(i for i in expanded if i["type"] == "IntegrationRuntime")
        m = extract_metrics(ir)
        assert m["ir_type"] == "Managed"

    def test_metrics_json_serializable(self, expanded):
        """All metrics should be JSON-serializable (for the key_metrics column)."""
        for item in expanded:
            m = extract_metrics(item)
            json_str = json.dumps(m, default=str)
            assert isinstance(json_str, str)
            parsed = json.loads(json_str)
            assert isinstance(parsed, dict)
