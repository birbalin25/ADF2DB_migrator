# ARM Template Format Reference

## ADF ARM Template Structure

An ADF ARM template export contains the complete factory definition in Azure Resource Manager format.

### Top-Level Structure

```json
{
  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "factoryName": {
      "type": "string",
      "metadata": "Data factory name",
      "defaultValue": "my-data-factory"
    }
  },
  "variables": {
    "factoryId": "[concat('Microsoft.DataFactory/factories/', parameters('factoryName'))]"
  },
  "resources": [
    { "...resource objects..." }
  ]
}
```

### Resource Object Structure

Each resource in the `resources[]` array follows this pattern:

```json
{
  "name": "[concat(parameters('factoryName'), '/ResourceName')]",
  "type": "Microsoft.DataFactory/factories/<resourceType>",
  "apiVersion": "2018-06-01",
  "properties": {
    "...type-specific properties..."
  },
  "dependsOn": [
    "[concat(variables('factoryId'), '/linkedservices/LinkedServiceName')]"
  ]
}
```

### Name Resolution

ARM names use the pattern `[concat(parameters('factoryName'), '/ComponentName')]`.

To extract the component name:
1. If the name matches `[concat(parameters('factoryName'), '/X')]` → extract `X`
2. If the name matches `factoryName/X` → extract `X`
3. For nested resources like managed private endpoints: `[concat(parameters('factoryName'), '/default/EndpointName')]` → extract `EndpointName`
4. Fallback: take the last `/`-delimited segment

### Resource Types

| ARM `type` field | Component Category |
|---|---|
| `Microsoft.DataFactory/factories/pipelines` | Pipeline |
| `Microsoft.DataFactory/factories/datasets` | Dataset |
| `Microsoft.DataFactory/factories/linkedservices` | LinkedService |
| `Microsoft.DataFactory/factories/triggers` | Trigger |
| `Microsoft.DataFactory/factories/dataflows` | DataFlow |
| `Microsoft.DataFactory/factories/integrationruntimes` | IntegrationRuntime |
| `Microsoft.DataFactory/factories/adfcdcs` | ChangeDataCapture |
| `Microsoft.DataFactory/factories/credentials` | Credential |
| `Microsoft.DataFactory/factories/globalParameters` | GlobalParameter |
| `Microsoft.DataFactory/factories/managedVirtualNetworks` | ManagedVirtualNetwork |
| `Microsoft.DataFactory/factories/managedVirtualNetworks/managedPrivateEndpoints` | ManagedPrivateEndpoint |
| `Microsoft.DataFactory/factories/privateEndpointConnections` | PrivateEndpointConnection |

## Pipeline Properties

```json
{
  "properties": {
    "activities": [
      {
        "name": "ActivityName",
        "type": "Copy",
        "dependsOn": [
          {
            "activity": "PreviousActivity",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "policy": {
          "timeout": "0.12:00:00",
          "retry": 3,
          "retryIntervalInSeconds": 30
        },
        "typeProperties": {
          "...activity-specific..."
        },
        "inputs": [{ "referenceName": "DatasetName", "type": "DatasetReference" }],
        "outputs": [{ "referenceName": "DatasetName", "type": "DatasetReference" }]
      }
    ],
    "parameters": {
      "paramName": { "type": "String", "defaultValue": "value" }
    },
    "variables": {
      "varName": { "type": "String", "defaultValue": "" }
    },
    "annotations": [],
    "folder": { "name": "FolderPath" }
  }
}
```

### Activity `dependsOn` Conditions

- `Succeeded` — Run only if predecessor succeeded
- `Failed` — Run only if predecessor failed
- `Completed` — Run regardless of predecessor outcome
- `Skipped` — Run only if predecessor was skipped

### Control Flow Activities

**ForEach**:
```json
{
  "type": "ForEach",
  "typeProperties": {
    "isSequential": false,
    "items": { "value": "@pipeline().parameters.tableList", "type": "Expression" },
    "batchCount": 20,
    "activities": [ "...inner activities..." ]
  }
}
```

**IfCondition**:
```json
{
  "type": "IfCondition",
  "typeProperties": {
    "expression": { "value": "@equals(activity('Lookup').output.count, 0)", "type": "Expression" },
    "ifTrueActivities": [ "..." ],
    "ifFalseActivities": [ "..." ]
  }
}
```

**Switch**:
```json
{
  "type": "Switch",
  "typeProperties": {
    "on": { "value": "@pipeline().parameters.env", "type": "Expression" },
    "cases": [
      { "value": "dev", "activities": [ "..." ] },
      { "value": "prod", "activities": [ "..." ] }
    ],
    "defaultActivities": [ "..." ]
  }
}
```

**Until**:
```json
{
  "type": "Until",
  "typeProperties": {
    "expression": { "value": "@equals(activity('Check').output.status, 'Complete')", "type": "Expression" },
    "timeout": "0.01:00:00",
    "activities": [ "..." ]
  }
}
```

## Copy Activity Properties

```json
{
  "type": "Copy",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT * FROM table WHERE date > '@{pipeline().parameters.startDate}'"
    },
    "sink": {
      "type": "ParquetSink",
      "storeSettings": { "type": "AzureBlobFSWriteSettings" },
      "formatSettings": { "type": "ParquetWriteSettings" }
    },
    "enableStaging": true,
    "stagingSettings": {
      "linkedServiceName": { "referenceName": "StagingBlobLS", "type": "LinkedServiceReference" },
      "path": "staging"
    },
    "translator": {
      "type": "TabularTranslator",
      "mappings": [
        { "source": { "name": "col1" }, "sink": { "name": "column_1" } }
      ]
    }
  }
}
```

## DataFlow Properties

```json
{
  "properties": {
    "type": "MappingDataFlow",
    "typeProperties": {
      "sources": [
        {
          "dataset": { "referenceName": "SourceDS", "type": "DatasetReference" },
          "name": "sourceStream"
        }
      ],
      "sinks": [
        {
          "dataset": { "referenceName": "SinkDS", "type": "DatasetReference" },
          "name": "sinkStream"
        }
      ],
      "transformations": [
        { "name": "filterRows" },
        { "name": "joinDim" },
        { "name": "aggMetrics" }
      ],
      "scriptLines": [
        "source(output(...), allowSchemaDrift: true) ~> sourceStream",
        "sourceStream filter(amount > 0) ~> filterRows",
        "filterRows, dimTable join(id == dim_id, joinType:'left') ~> joinDim",
        "joinDim aggregate(groupBy(category), total = sum(amount)) ~> aggMetrics",
        "aggMetrics sink(allowSchemaDrift: true) ~> sinkStream"
      ]
    }
  }
}
```

## Trigger Properties

**ScheduleTrigger**:
```json
{
  "properties": {
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "startTime": "2024-01-01T00:00:00Z",
        "timeZone": "UTC",
        "schedule": {
          "hours": [6],
          "minutes": [0]
        }
      }
    },
    "pipelines": [
      {
        "pipelineReference": { "referenceName": "PipelineName", "type": "PipelineReference" },
        "parameters": { "env": "prod" }
      }
    ]
  }
}
```

**TumblingWindowTrigger**:
```json
{
  "properties": {
    "type": "TumblingWindowTrigger",
    "typeProperties": {
      "frequency": "Hour",
      "interval": 1,
      "startTime": "2024-01-01T00:00:00Z",
      "delay": "00:15:00",
      "maxConcurrency": 5,
      "retryPolicy": { "count": 3, "intervalInSeconds": 30 }
    },
    "pipeline": {
      "pipelineReference": { "referenceName": "PipelineName", "type": "PipelineReference" },
      "parameters": {}
    }
  }
}
```

**BlobEventsTrigger**:
```json
{
  "properties": {
    "type": "BlobEventsTrigger",
    "typeProperties": {
      "blobPathBeginsWith": "/container/path/",
      "blobPathEndsWith": ".csv",
      "ignoreEmptyBlobs": true,
      "scope": "/subscriptions/.../storageAccounts/accountName",
      "events": ["Microsoft.Storage.BlobCreated"]
    }
  }
}
```

## LinkedService Properties

```json
{
  "properties": {
    "type": "AzureSqlDatabase",
    "typeProperties": {
      "connectionString": {
        "type": "AzureKeyVaultSecret",
        "store": { "referenceName": "AzureKeyVaultLS", "type": "LinkedServiceReference" },
        "secretName": "sql-connection-string"
      }
    },
    "connectVia": {
      "referenceName": "AutoResolveIR",
      "type": "IntegrationRuntimeReference"
    }
  }
}
```

## Dataset Properties

```json
{
  "properties": {
    "type": "AzureSqlTable",
    "linkedServiceName": {
      "referenceName": "AzureSqlLS",
      "type": "LinkedServiceReference"
    },
    "typeProperties": {
      "schema": "dbo",
      "table": "TableName"
    },
    "schema": [
      { "name": "id", "type": "int" },
      { "name": "name", "type": "nvarchar" }
    ],
    "parameters": {
      "tableName": { "type": "String" }
    }
  }
}
```

## Integration Runtime Properties

**Managed (Azure IR)**:
```json
{
  "properties": {
    "type": "Managed",
    "typeProperties": {
      "computeProperties": {
        "location": "AutoResolve",
        "dataFlowProperties": {
          "computeType": "General",
          "coreCount": 8,
          "timeToLive": 10
        }
      }
    }
  }
}
```

**Self-Hosted**:
```json
{
  "properties": {
    "type": "SelfHosted",
    "typeProperties": {}
  }
}
```
