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

package googlesource

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"go.uber.org/goleak"
	"google.golang.org/api/option"
)

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            "table_acceptance",
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigPrimaryKeyColName:  "name",
		googlebigquery.ConfigIncrementalColName: "name",
		googlebigquery.ConfigPollingTime:        "1ms",
	}

	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		Config: AcceptanceSourceTestDriverConfig{
			Connector: sdk.Connector{
				NewSpecification: googlebigquery.Specification,
				NewSource:        NewSource,
			},
			SourceConfig: cfg,
			GoleakOptions: []goleak.Option{
				goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"), // indirect leak from dependency go.opencensus.io
				goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
			},
			BeforeTest: func(t *testing.T) {
				err := createDatasetForAcceptance(t, cfg[googlebigquery.ConfigTableID])
				if err != nil {
					fmt.Println("Error found")
				}
			},
			AfterTest: func(t *testing.T) {
				err := cleanupDataset(t, []string{cfg[googlebigquery.ConfigTableID]})
				if err != nil {
					fmt.Println("Error found")
				}
			},
		},
	},
	)
}

// AcceptanceTestDriver implements sdk.AcceptanceTestDriver
type AcceptanceTestDriver struct {
	Config AcceptanceSourceTestDriverConfig
}

// AcceptanceSourceTestDriverConfig contains the configuration for
// AcceptanceTestDriver.
type AcceptanceSourceTestDriverConfig struct {
	// Connector is the connector to be tested.
	Connector sdk.Connector

	// SourceConfig config for source
	SourceConfig map[string]string

	// BeforeTest is executed before each acceptance test.
	BeforeTest func(t *testing.T)
	// AfterTest is executed after each acceptance test.
	AfterTest func(t *testing.T)

	// GoleakOptions will be applied to goleak.VerifyNone. Can be used to
	// suppress false positive goroutine leaks.
	GoleakOptions []goleak.Option

	// Skip is a slice of regular expressions used to identify tests that should
	// be skipped. The full test name will be matched against all regular
	// expressions and the test will be skipped if a match is found.
	Skip []string
}

func (d AcceptanceTestDriver) DestinationConfig(*testing.T) map[string]string {
	return map[string]string{}
}
func (d AcceptanceTestDriver) Connector() sdk.Connector {
	return d.Config.Connector
}

func (d AcceptanceTestDriver) SourceConfig(*testing.T) map[string]string {
	return d.Config.SourceConfig
}

func (d AcceptanceTestDriver) BeforeTest(t *testing.T) {
	// before test check if the test should be skipped
	d.Skip(t)

	if d.Config.BeforeTest != nil {
		d.Config.BeforeTest(t)
	}
}

func (d AcceptanceTestDriver) AfterTest(t *testing.T) {
	if d.Config.AfterTest != nil {
		d.Config.AfterTest(t)
	}
}

func (d AcceptanceTestDriver) Skip(t *testing.T) {
	var skipRegexs []*regexp.Regexp
	for _, skipRegex := range d.Config.Skip {
		r := regexp.MustCompile(skipRegex)
		skipRegexs = append(skipRegexs, r)
	}

	for _, skipRegex := range skipRegexs {
		if skipRegex.MatchString(t.Name()) {
			t.Skip(fmt.Sprintf("caller requested to skip tests that match the regex %q", skipRegex.String()))
		}
	}
}

func (d AcceptanceTestDriver) GoleakOptions(_ *testing.T) []goleak.Option {
	return d.Config.GoleakOptions
}

// WriteToSource writes data for source to pull data from
func (d AcceptanceTestDriver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
	var err error
	is := is.New(t)
	config := d.SourceConfig(t)
	records, err = writeToSource(t, config, records)
	is.NoErr(err)

	return records
}

func writeToSource(t *testing.T, config map[string]string, records []sdk.Record) (result []sdk.Record, err error) {
	result, err = dataSetupWithRecord(t, config, records)
	return result, err
}
func (d AcceptanceTestDriver) ReadFromDestination(*testing.T, []sdk.Record) []sdk.Record {
	return []sdk.Record{}
}

func (d AcceptanceTestDriver) GenerateRecord(_ *testing.T) sdk.Record {
	// we don't create record over here. Because we need to code here and then decode again
	return sdk.Record{}
}

func createDatasetForAcceptance(t *testing.T, tableID string) (err error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location: location, // See https://cloud.google.com/bigquery/docs/locations
	}

	// create dataset
	if err := client.Dataset(datasetID).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "duplicate") {
		return err
	}
	t.Log("Dataset created")
	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	sampleSchema := bigquery.Schema{
		{Name: "abb", Type: bigquery.StringFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         sampleSchema,
		ExpirationTime: time.Now().AddDate(1, 0, 0), // Table will be automatically deleted in 1 year.
	}
	tableRef := client.Dataset(datasetID).Table(tableID)
	err = tableRef.Create(ctx, metaData)
	if err != nil && !strings.Contains(err.Error(), "duplicate") {
		return err
	}
	return
}

// dataSetupWithRecord Initial setup required - project with service account.
func dataSetupWithRecord(t *testing.T, config map[string]string, record []sdk.Record) (result []sdk.Record, err error) {
	ctx := context.Background()
	tableID := config[googlebigquery.ConfigTableID]

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return result, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	var query string
	positions := ""

	for i := 0; i < len(record); i++ {
		name := fmt.Sprintf("name%v", time.Now().AddDate(0, 0, globalCounter).Format("20060102150405"))
		abb := fmt.Sprintf("name%v", time.Now().AddDate(0, 0, globalCounter).Format("20060102150405"))
		query = "INSERT INTO `" + projectID + "." + datasetID + "." + tableID + "`  values ('" + abb + "' , '" + name + "')"

		data := make(sdk.StructuredData)
		data["abb"] = abb
		data["name"] = name

		key := name

		buffer := &bytes.Buffer{}
		if err := gob.NewEncoder(buffer).Encode(key); err != nil {
			return result, err
		}
		byteKey := buffer.Bytes()

		positions = fmt.Sprintf("'%s'", name)
		positionRecord, err := json.Marshal(&positions)
		if err != nil {
			t.Log("error found", err)
			return result, err
		}

		result = append(result, sdk.Record{Payload: data, Key: sdk.RawData(byteKey), Position: positionRecord})
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
