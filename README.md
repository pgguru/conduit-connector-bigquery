### Conduit Connector for BigQuery
[Conduit](https://conduit.io) for BigQuery.

#### Source
A source connector pulls data from BigQuery and pushes it to downstream resources via Conduit.

### Implementation
The connector pulls data from BigQuery for a dataset or selected tables of users choice. The connector syncs incrementally this means
it keeps on looking for new insertion/updation happening every time interval user specified in any of the table the data is pulled for and syncs it. 
If the Conduit stops or pauses midway the connector will make sure to pull the data which was not pull earlier. 

for eg,
- table A and table B are synced.
- Pipeline is paused after syncing complete table A and table B till index 5.
- On resuming the pipeline - Connector sync data from table B index 6 and would not sync table A's already synced rows.

### How to build?
Run `make build` to build the connector.

### Configuration
| name |  description | required | default value |
|------|--------------|----------|---------------|
|`serviceAccount`| service account with access to project. ref: https://cloud.google.com/docs/authentication/getting-started|true| - |
|`projectID`| The Project ID on endpoint|true| - |
|`datasetID`|The dataset ID to pull data from.|true| - |
|`tableID`|Specify comma separated table IDs. Will pull whole dataset if no Table ID present. |false|all tables in dataset|
|`datasetLocation`|Specify location were dataset exist|true| - |
|`pollingTime`|Specify time foramtted as a time.Duration string, after which polling of data should be done. For eg, "2s", "500ms"|false|5m|
|`incrementingColumnName`|Specify the column name which provide visibility about newer row or newer updates. It can be either `updated_at` timestamp which specifies when the table was last updated. It can be a `ID` of type int or float whose value increases with every new record coming in. User need to provide column name for table in a format - 'columnName' without any spaces Eg: 'created_by' where created_by is column name. Table with no value will be pulled without any ordering.|false| - |
|`primaryKeyColName`|Specify the primary key column name. eg, `ID` of type int or float or any primary key. User need to provide column name for each table in a format - 'columnName' without any spaces Eg: 'created_by' where created_by is column name. |true| - |

### How to configure
Create a connector using - `POST /v1/connectors` API

Sample payload-
```json
{
  "config": {
    "name": "google_bigQuery",
    "settings": {
      "datasetID": "<dataset_name>",
      "datasetLocation": "<location_of_dataset eg, US>",
      "projectID": "<project_name eg, conduit-connectors>",
      "serviceAccount": "<path_to_key eg, /home/username/conduit-connectors-cf3466b16662.json>",
      "tableID": "< table_names_to_sync eg, table_1,table_2>",
      "incrementingColumnName": "<table_name:incrementingColName,table_name2:incrementingColName2... eg, table1:id,table2:updatedat>",
      "primaryKeyColName": "<table_name:primaryColName,table_name2:primaryColName2... eg, table1:id,table2:_id>",
      "pollingTime": "<time in duration eg,1m20s>"
    }
  },
  "type": "TYPE_SOURCE",
  "plugin": "standalone:bigquery", // the connector's binary should be put into a directory that holds all the standalone connectors as described here https://github.com/ConduitIO/conduit/issues/427#issuecomment-1227396725.
  "pipelineId": "<ID of pipeline created eg, 8cb0e678-d797-401c-8d23-f1e9e859b885>"
}
```

### Testing
Run `make test` to run all the unit tests. To run the test cases export environment variable - `GOOGLE_SERVICE_ACCOUNT` and `GOOGLE_PROJECT_ID` where,
- `GOOGLE_SERVICE_ACCOUNT` is the value in google service account file.  refer: https://cloud.google.com/docs/authentication/getting-started to create a service account
- `GOOGLE_PROJECT_ID` is  the ID of projects whose tables data is to be synced

### Known Issues & Limitations
* Current implementation handles snapshot and incremental data.
* The connector is able to send record's `Key` as `sdk.RawData` only.

### Planned work
- [ ] Handle Delete data

### Specification
The `spec.go` file provides a programmatic representation of the configuration options. This is used by the Conduit
server to validate configuration and dynamically display configuration options to end users.
