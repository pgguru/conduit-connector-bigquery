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
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
)

var (
	serviceAccount   = os.Getenv("SERVICE_ACCOUNT") // eg, export SERVICE_ACCOUNT = "path_to_file"
	projectID        = os.Getenv("PROJECT_ID")      // eg, export PROJECT_ID ="conduit-connectors"
	datasetID        = "conduit_test_dataset"
	tableID          = "conduit_test_table"
	tableID2         = "conduit_test_table_2"
	tableIDTimeStamp = "conduit_test_table_time_stamp"
	location         = "US"
)

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

// Item represents a row item.
type Item struct {
	Name      string
	Age       int
	Updatedat time.Time
}

// Initial setup required - project with service account.
func dataSetupWithTimestamp() (err error) {
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

	sampleSchema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
		{Name: "updatedat", Type: bigquery.TimestampFieldType},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         sampleSchema,
		ExpirationTime: time.Now().AddDate(1, 0, 0), // Table will be automatically deleted in 1 year.
	}
	tableRef := client.Dataset(datasetID).Table(tableIDTimeStamp)
	err = tableRef.Create(ctx, metaData)
	fmt.Println("Error: ", err)
	if err != nil && !strings.Contains(err.Error(), "duplicate") {
		return err
	}

	var query string

	for i := 0; i < 8; i++ {
		iString := fmt.Sprintf("%d", i)
		query = "INSERT INTO `" + projectID + "." + datasetID + "." + tableIDTimeStamp + "`  values ('name" + iString +
			"', " + iString + ", '" + time.Now().UTC().AddDate(0, 0, -i).Format("2006-01-02 15:04:05.999999 MST") + "' )"

		q := client.Query(query)
		q.Location = location

		job, err := q.Run(ctx)
		if err != nil {
			log.Println("Error found: ", err)
		}

		status, err := job.Wait(ctx)
		if err != nil {
			log.Println("Error found: ", err)
		}

		if err := status.Err(); err != nil {
			log.Println("Error found: ", err)
		}
	}

	return nil
}

func dataUpdationWithTimestamp() {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		log.Println("Error found: ", err)
	}
	defer client.Close()

	query := "UPDATE `" + projectID + "." + datasetID + "." + tableIDTimeStamp + "` " +
		" SET updatedat='" + time.Now().UTC().AddDate(0, 0, +10).Format("2006-01-02 15:04:05.999999 MST") + "' WHERE name =" + "'name4';"

	q := client.Query(query)
	q.Location = location

	job, err := q.Run(ctx)
	if err != nil {
		log.Println("Error found: ", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Println("Error found: ", err)
	}

	if err := status.Err(); err != nil {
		log.Println("Error found: ", err)
	}
}

func cleanupDataset(tables []string) (err error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	for _, tableID := range tables {
		table := client.Dataset(datasetID).Table(tableID)
		err := table.Delete(ctx)
		if err != nil && strings.Contains(err.Error(), "Not found") {
			return err
		}
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

func TestSuccessTimeIncremental(t *testing.T) {
	err := dataSetupWithTimestamp()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableIDTimeStamp})
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            tableIDTimeStamp,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigIncrementalColName: fmt.Sprintf("%s:updatedat", tableIDTimeStamp),
		googlebigquery.ConfigPrimaryKeyColName:  fmt.Sprintf("%s:age", tableIDTimeStamp),
	}
	googlebigquery.PollingTime = time.Second * 1

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}
	pos := sdk.Position{}

	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}
	time.Sleep(15 * time.Second)
	for {
		record, err := src.Read(ctx)
		if err != nil || ctx.Err() != nil {
			fmt.Println(err)
			break
		}

		value := string(record.Position)
		fmt.Printf("Record position found: %s", value)

		value = string(record.Payload.Bytes())
		fmt.Println(" :", value)

		value = string(record.Key.Bytes())
		fmt.Println("Key :", value)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessTimeIncrementalAndUpdate(t *testing.T) {
	err := dataSetupWithTimestamp()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableIDTimeStamp})
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            tableIDTimeStamp,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigIncrementalColName: fmt.Sprintf("%s:updatedat", tableIDTimeStamp),
		googlebigquery.ConfigPrimaryKeyColName:  fmt.Sprintf("%s:age", tableIDTimeStamp),
	}
	googlebigquery.PollingTime = time.Second * 1

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}
	pos := sdk.Position{}

	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}

	var recordPos []byte

	time.Sleep(15 * time.Second)
	var record sdk.Record
	for {
		record, err = src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			fmt.Println(err)
			break
		}

		value := string(record.Position)
		fmt.Printf("Record position found: %s", value)

		value = string(record.Payload.Bytes())
		fmt.Println(" :", value)

		value = string(record.Key.Bytes())
		fmt.Println("Key :", value)
		recordPos = record.Position
	}

	// check updated
	dataUpdationWithTimestamp()

	err = src.Open(ctx, recordPos)
	if err != nil {
		fmt.Println("errror: ", err)
	}
	time.Sleep(15 * time.Second)
	for {
		record, err = src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			fmt.Println(err)
			break
		}

		value := string(record.Position)
		fmt.Printf("After 1st read: Record position found: %s", value)

		value = string(record.Payload.Bytes())
		fmt.Println(" :", value)

		value = string(record.Key.Bytes())
		fmt.Println("Key :", value)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessPrimaryKey(t *testing.T) {
	err := dataSetupWithTimestamp()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableIDTimeStamp})
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            tableIDTimeStamp,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigIncrementalColName: fmt.Sprintf("%s:age", tableIDTimeStamp),
		googlebigquery.ConfigPrimaryKeyColName:  fmt.Sprintf("%s:updatedat", tableIDTimeStamp),
	}
	googlebigquery.PollingTime = time.Second * 1

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}
	pos := sdk.Position{}

	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}
	time.Sleep(15 * time.Second)
	for {
		record, err := src.Read(ctx)
		if err != nil || ctx.Err() != nil {
			fmt.Println(err)
			break
		}

		value := string(record.Position)
		fmt.Printf("Record position found: %s", value)

		value = string(record.Payload.Bytes())
		fmt.Println(" :", value)
		value = string(record.Key.Bytes())
		fmt.Println("Key :", value)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessfulGet(t *testing.T) {
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableID, tableID2})
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount: serviceAccount,
		googlebigquery.ConfigProjectID:      projectID,
		googlebigquery.ConfigDatasetID:      datasetID,
		googlebigquery.ConfigTableID:        tableID,
		googlebigquery.ConfigLocation:       location,
	}
	googlebigquery.PollingTime = time.Second * 1

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}
	positionMap := make(map[string]string)
	positionMap["conduit_test_table"] = "46"
	pos, err := json.Marshal(&positionMap)
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
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableID, tableID2})
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            fmt.Sprintf("%s,%s", tableID, tableID2), // tableID,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigIncrementalColName: fmt.Sprintf("%s:post_abbr", tableID)}

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
		fmt.Println("\n* ")
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessfulOrderByName(t *testing.T) {
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableID, tableID2})
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigIncrementalColName: "conduit_test_table:post_abbr",
	}

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
