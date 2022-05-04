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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
)

var (
	serviceAccount = "<replace_me>"       // replace with path to service account with permission for the project
	projectID      = "conduit-connectors" // replace projectID created
	datasetID      = "conduit_test_dataset"
	tableID        = "conduit_test_table"
	tableID2       = "conduit_test_table_2"
	location       = "US"
)

func TestDataSetup(t *testing.T) {
	dataSetup()
}

// Initial setup required - project with service account.
func dataSetup() (err error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
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
	fmt.Println("Dataset created")
	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference("gs://cloud-samples-data/bigquery/us-states/us-states.json")
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.Schema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "post_abbr", Type: bigquery.StringFieldType},
	}
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteEmpty

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil && !strings.Contains(status.Err().Error(), "duplicate") {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}

	fmt.Println("Table created:", tableID)
	// create another table

	loader = client.Dataset(datasetID).Table(tableID2).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteEmpty

	job, err = loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err = job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil && !strings.Contains(status.Err().Error(), "duplicate") {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}

	fmt.Println("Table created:", tableID2)

	return nil
}

func cleanupDataSet() (err error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	table := client.Dataset(datasetID).Table(tableID)
	if err := table.Delete(ctx); err != nil {
		return err
	}

	table = client.Dataset(datasetID).Table(tableID2)
	if err := table.Delete(ctx); err != nil {
		return err
	}

	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	if err = client.Dataset(datasetID).Delete(ctx); err != nil {
		fmt.Println("Error in delete: ", err)
		return err
	}
	return err
}

func TestSuccessfulGet(t *testing.T) {
	// cleanupDataSet()
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataSet()
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = serviceAccount
	cfg[googlebigquery.ConfigProjectID] = projectID
	cfg[googlebigquery.ConfigDatasetID] = datasetID
	cfg[googlebigquery.ConfigTableID] = tableID
	cfg[googlebigquery.ConfigLocation] = location

	googlebigquery.PollingTime = time.Second * 1

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}

	pos, err := json.Marshal(Position{TableID: "conduit_test_table", Offset: 46})
	if err != nil {
		fmt.Println(err)
	}

	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}
	time.Sleep(15 * time.Second)
	for i := 0; i <= 4; i++ {
		record, err := src.Read(ctx)
		if err != nil || ctx.Err() != nil {
			fmt.Println(err)
			break
		}

		value := string(record.Position)
		fmt.Printf("Record position found: %s", value)

		value = string(record.Payload.Bytes())
		fmt.Println(" :", value)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessfulGetWholeDataset(t *testing.T) {
	// cleanupDataSet()
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataSet()
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = serviceAccount
	cfg[googlebigquery.ConfigProjectID] = projectID
	cfg[googlebigquery.ConfigDatasetID] = datasetID
	cfg[googlebigquery.ConfigLocation] = location

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
		t.Errorf("some other error found: %v", err)
	}

	googlebigquery.PollingTime = time.Second * 1
	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
		t.Errorf("some other error found: %v", err)
	}
	time.Sleep(10 * time.Second)

	for {
		record, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			fmt.Println("err: ", err)
			break
		}
		if err != nil {
			t.Errorf("some other error found: %v", err)
		}
		value := string(record.Position)
		fmt.Println("Record found:", value)
		value = string(record.Payload.Bytes())
		fmt.Println(":", value)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
