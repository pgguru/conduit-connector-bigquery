### Conduit Connector for BigQuery
[Conduit](https://conduit.io) for BigQuery.

#### Source
A source connector pulls data from bigquery and pushes it to downstream resources via Conduit.

### Implementation
The connector pulls data from bigquery for a dataset or selected tables of users choice. The connector syncs incrementally this means
it keeps on looking for new insertion/updation happening every time interval user specified in any of the table the data is pulled for and syncs it. 
If the conduit stops or pauses midway the connector will make sure to pull the data which was not pull earlier. 

for eg,
- table A and table B are synced.
- Pipeline is paused after syncing complete table A and table B till index 5.
- On resuming the pipeline - Connector sync data from table B index 6 and would not sync table A's already synced rows.

### How to build?
Run `make build` to build the connector.

### Configuration
[comment]: <> (fix table format and spacing)
| name |  description | required | default value |
|------|--------------|----------|---------------|
|`serviceAccount`|Path to service account file with access to project|true| false|
|`projectID`| The Project ID on endpoint|true| false|
|`datasetID`|The dataset ID to pull data from.|true|false|
|`tableID`|Specify comma separated table IDs. Will pull whole dataset if no Table ID present. |false|false|
|`datasetLocation`|Specify location were dataset exist|true|false|
|`pollingTime`|Specify time foramtted as a time.Duration string, after which polling of data should be done. For eg, "2s", "500ms"|false|true|
|`incrementingColumnName`|Specify the column name which provide visibility about newer row or newer updates. It can be either `updated_at` timestamp which specifies when the table was last updated. It can be a `ID` of type int or float whose value increases with every new record coming in. User need to provide column name for each table in a format - 'tableID:columnName,tableID:columnName' without any spaces Eg: 'table1:created_by,table2:id' where created_by and id are column names. Table with no value will be pulled without any ordering.|false|false|
|`primaryKeyColName`|Specify the primary key column name. eg, `ID` of type int or float or any primary key. User need to provide column name for each table in a format - 'tableID:columnName,tableID:columnName' without any spaces Eg: 'table1:created_by,table2:id' where created_by and id are column names. Table with no value will be pulled without any ordering. This field is recommended|false|false|

### How to configure
Create a connector using - `POST /v1/connectors` API

Sample payload-
```{
   "config": {
     "name": "google_bigQuery",
     "settings": {
       "datasetID": "<dataset_name>",
       "datasetLocation": "<location_of_dataset eg, US>",
       "projectID": "<project_name eg, conduit-connectors>",
"serviceAccount": "<path_to_key eg, /home/username/conduit-connectors-cf3466b16662.json>",
       "tableID": "< table_names_to_sync eg, table_1,table_2>",
       "incrementingColumnName":"<table_name:incrementingColName,table_name2:incrementingColName2... eg, table1:id,table2:updatedat>",
       "primaryKeyColName":"<table_name:primaryColName,table_name2:primaryColName2... eg, table1:id,table2:_id>",
       "pollingTime":"<time in duration eg,1m20s>"
     }
   },
   "type": "TYPE_SOURCE",
"plugin": "<path to plugin which was created by make command eg, /home/username/downloads/conduit-connector-bigquery/conduit-connector-bigquery>",
   "pipelineId": "<ID of pipeline created eg, 8cb0e678-d797-401c-8d23-f1e9e859b885>"
 
 }
 ```

### Testing
Run `make test` to run all the unit tests. To run the test cases export - `SERVICE_ACCOUNT` and `PROJECT_ID`

### Known Issues & Limitations
* Current implementation handles snapshot and incremental data.

### Planned work
- [ ] Handle Delete data

### Specification
The `spec.go` file provides a programmatic representation of the configuration options. This is used by the Conduit
server to validate configuration and dynamically display configuration options to end users.
