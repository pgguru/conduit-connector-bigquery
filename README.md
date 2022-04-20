### Conduit Connector for BigQuery
[Conduit](https://conduit.io) for BigQuery.

#### Source
A source connector pulls data from bigquery and pushes it to downstream resources via Conduit.

### How to build?
Run `make build` to build the connector.

### Testing
Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests. To run the test cases update - `serviceAccount` and `projectID`
in `google_source->source_intergration_test.go` 

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

### Known Issues & Limitations
* Current implementation handles snapshot data.

### Planned work
- [ ] Handle CDC data

### Specification
The `spec.go` file provides a programmatic representation of the configuration options. This is used by the Conduit
server to validate configuration and dynamically display configuration options to end users.

### Configuration

| name |  description | required | default value |
|------|---------|-------------|----------|
|`serviceAccount`|Path to service account file with access to project|true| false|
|`srcProjectID`| The Project ID on endpoint|true| false|
|`srcDatasetID`|The dataset ID to pull data from.|true|false|
|`srcTableID`|Specify comma separated table IDs. Will pull whole dataset if no Table ID present. |false|false|
