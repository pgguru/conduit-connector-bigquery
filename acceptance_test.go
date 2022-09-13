// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlebigquery

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/conduitio-labs/conduit-connector-bigquery/config"
	"github.com/conduitio-labs/conduit-connector-bigquery/googlesource"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
)

var (
	globalCounter  = 0
	serviceAccount = os.Getenv("GOOGLE_SERVICE_ACCOUNT") // eg, export GOOGLE_SERVICE_ACCOUNT = "path to service account file"
	projectID      = os.Getenv("GOOGLE_PROJECT_ID")      // eg, export GOOGLE_PROJECT_ID ="conduit-connectors"
	datasetID      = "conduit_test_dataset"
	location       = "US"
)

func TestAcceptance(t *testing.T) {
	if serviceAccount == "" || projectID == "" {
		t.Skip("GOOGLE_SERVICE_ACCOUNT or GOOGLE_PROJECT_ID is missing")
	}

	cfg := map[string]string{
		config.KeyServiceAccount:     serviceAccount,
		config.KeyProjectID:          projectID,
		config.KeyDatasetID:          datasetID,
		config.KeyTableID:            "table_acceptance",
		config.KeyLocation:           location,
		config.KeyPrimaryKeyColName:  "created_at",
		config.KeyIncrementalColName: "created_at",
		config.KeyPollingTime:        "1ms",
	}

	// create a dataset once and clean up later
	client, err := createDataSetForAcceptance(t)
	if err != nil {
		t.Fatalf("could not create dataset. err %v", err)
	}

	defer func() {
		err := cleanupDatasetForAcceptance(t, client)
		if err != nil {
			t.Log("Error while deleting dataset. err: ", err)
		}
	}()

	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: Specification,
					NewSource:        googlesource.NewSource,
				},
				SourceConfig: cfg,
				GoleakOptions: []goleak.Option{
					goleak.IgnoreCurrent(),
					goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"), // indirect leak from dependency go.opencensus.io
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
				},
				BeforeTest: func(t *testing.T) {
					err := createTableForAcceptance(t, client, cfg[config.KeyTableID])
					if err != nil {
						t.Log("Error found")
					}
				},
				AfterTest: func(t *testing.T) {
					err := cleantUpTableForAcceptance(t, client, []string{cfg[config.KeyTableID]})
					if err != nil {
						t.Log("Error found")
					}
				},
			},
		},
	},
	)
}

// AcceptanceTestDriver implements sdk.AcceptanceTestDriver
type AcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

func (d AcceptanceTestDriver) GenerateRecord(t *testing.T, op sdk.Operation) sdk.Record {
	record := d.ConfigurableAcceptanceTestDriver.GenerateRecord(t, op)

	record.Metadata = nil

	return record
}

// WriteToSource writes data for source to pull data from
func (d AcceptanceTestDriver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
	var err error
	is := is.New(t)
	config := d.ConfigurableAcceptanceTestDriver.SourceConfig(t)
	records, err = writeToSource(t, config, records)
	is.NoErr(err)

	return records
}

func writeToSource(t *testing.T, config map[string]string, records []sdk.Record) (result []sdk.Record, err error) {
	result, err = dataSetupWithRecord(t, config, records)
	return result, err
}

func createDataSetForAcceptance(t *testing.T) (client *bigquery.Client, err error) {
	ctx := context.Background()

	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return client, fmt.Errorf("bigquery.NewClient: %v", err)
	}

	meta := &bigquery.DatasetMetadata{
		Location: location, // See https://cloud.google.com/bigquery/docs/locations
	}

	// create dataset
	if err := client.Dataset(datasetID).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "duplicate") {
		return client, err
	}
	t.Log("Dataset created")
	return client, err
}

func createTableForAcceptance(t *testing.T, client *bigquery.Client, tableID string) (err error) {
	ctx := context.Background()

	sampleSchema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "created_at", Type: bigquery.TimestampFieldType},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         sampleSchema,
		ExpirationTime: time.Now().Add(1 * time.Hour), // Table will be automatically deleted in 1 year.
	}
	tableRef := client.Dataset(datasetID).Table(tableID)
	err = tableRef.Create(ctx, metaData)
	if err != nil && !strings.Contains(err.Error(), "duplicate") {
		return err
	}
	return
}

// dataSetupWithRecord Initial setup required - project with service account.
func dataSetupWithRecord(t *testing.T, cfg map[string]string, record []sdk.Record) (result []sdk.Record, err error) {
	ctx := context.Background()
	tableID := cfg[config.KeyTableID]

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return result, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	var query string
	positions := ""

	for i := 0; i < len(record); i++ {
		createdAt := time.Now().AddDate(0, 0, globalCounter).UTC()
		name := fmt.Sprintf("name%v", globalCounter)
		createdAtBQFormat := createdAt.Format("2006-01-02 15:04:05.999999 MST")

		query = "INSERT INTO `" + projectID + "." + datasetID + "." + tableID + "`  values ('" + name + "' , '" + createdAtBQFormat + "')"

		data := make(sdk.StructuredData)
		data["name"] = name
		data["created_at"] = createdAtBQFormat

		key := createdAtBQFormat

		buffer := &bytes.Buffer{}
		if err := gob.NewEncoder(buffer).Encode(key); err != nil {
			return result, err
		}
		byteKey := buffer.Bytes()

		positions = fmt.Sprintf("'%s'", createdAtBQFormat)
		positionRecord, err := json.Marshal(&positions)
		if err != nil {
			t.Log("error found", err)
			return result, err
		}

		newRecord := sdk.Util.Source.NewRecordCreate(
			positionRecord, nil, sdk.RawData(byteKey), data,
		)
		newRecord.Metadata = nil

		result = append(result, newRecord)

		q := client.Query(query)
		q.Location = location

		job, err := q.Run(ctx)
		if err != nil {
			t.Log("Error found: ", err)
		}

		status, err := job.Wait(ctx)
		if err != nil {
			t.Log("Error found: ", err)
			return result, err
		}

		if err = status.Err(); err != nil {
			t.Log("Error found: ", err)
		}
		globalCounter++
	}
	return result, nil
}

func cleantUpTableForAcceptance(t *testing.T, client *bigquery.Client, tables []string) (err error) {
	ctx := context.Background()

	for _, tableID := range tables {
		table := client.Dataset(datasetID).Table(tableID)
		err := table.Delete(ctx)
		if err != nil && strings.Contains(err.Error(), "Not found") {
			return err
		}
	}
	return err
}

func cleanupDatasetForAcceptance(t *testing.T, client *bigquery.Client) (err error) {
	defer client.Close()
	ctx := context.Background()
	if err = client.Dataset(datasetID).Delete(ctx); err != nil {
		// dataset could already be in use. it is okay if it does not get deleted
		t.Log("Error in delete: ", err)
		return err
	}
	return err
}
