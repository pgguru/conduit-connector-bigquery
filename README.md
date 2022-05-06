### Conduit Connector for BigQuery
[Conduit](https://conduit.io) for BigQuery.

#### Source
A source connector pulls data from bigquery and pushes it to downstream resources via Conduit.

### Implementation
The connector pulls data from bigquery for a dataset or selected tables of users choice. The connector syncs incrementally this means
it keeps on looking for new insertion happening every minure in any of the table the data is pulled for and syncs it. 
If the conduit stops or pauses midway the connector will make sure to pull the data which was not pull earlier. 

for eg,
- table A and table B are synced.
- Pipeline is paused after syncing complete table A and table B till index 5.
- On resuming the pipeline - Connector sync data from table B index 6 and would not sync table A's already synced rows.

// haris: it would be good to describe why this isn't handled (technical limitation, simply didn't get to this)
// maybe even add to the planned work section, so we have it in one place 
// Neha: The endpoint does not provide any direct way of pulling the updates. I did try the following ways-
// 1. Use transaction/jobs to check the changes done - This works in case of queries but fails if it was a `load job`
// 2. Went through postgres connector, they use data replication feature to get the changes. But google being a fault tolerant warehouse does
//    not provide any such feature of replication
// 3. Different endpoint generally provide the feature of getting the delta. Bigquery does not provide any such feature because Bigquery is 
//    similar to any sql database where to pull the data for a interval we need to add some column of timestamp and then use it for getting updated
//    data. But since we are working on the metadata of table and not table its not possible to get the delta data.
Currently updataion/deletion is not handled by the connector.

### How to build?
Run `make build` to build the connector.

### Testing
Run `make test` to run all the unit tests. To run the test cases update - `serviceAccount` and `projectID`
in `google_source->source_intergration_test.go` 

### Known Issues & Limitations
* Current implementation handles snapshot and incremental data.

### Planned work
- [ ] Handle CDC data

### Specification
The `spec.go` file provides a programmatic representation of the configuration options. This is used by the Conduit
server to validate configuration and dynamically display configuration options to end users.

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
|`orderby`|Specify the column name by which the data should be ordered. User need to provide column name for each table in a format - 'tableID:columnName,tableID:columnName' without any spaces Eg: 'table1:created_by,table2:name' where created_by and name are column names. Table with no value will be pulled without any ordering.|true|false|

 