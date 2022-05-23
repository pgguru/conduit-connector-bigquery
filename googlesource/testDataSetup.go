package googlesource

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
)

var (
	serviceAccount = "/home/nehagupta/Downloads/conduit-connectors-cf3466b16662.json" // eg, export SERVICE_ACCOUNT = "path_to_file"
	projectID      = "conduit-connectors"

	// serviceAccount   = os.Getenv("SERVICE_ACCOUNT") // eg, export SERVICE_ACCOUNT = "path_to_file"
	// projectID        = os.Getenv("PROJECT_ID")      // eg, export PROJECT_ID ="conduit-connectors"
	datasetID        = "conduit_test_dataset"
	tableID          = "conduit_test_table"
	tableID2         = "conduit_test_table_2"
	tableIDTimeStamp = "conduit_test_table_time_stamp"
	location         = "US"
)

func DataSetup() (err error) {
	err = dataSetup()
	if err != nil {
		return err
	}
	return err
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
	time.Sleep(time.Second * 10)
	fmt.Println("Table created:", tableID2)

	return nil
}

// dataSetupWithRecord Initial setup required - project with service account.
func dataSetupWithRecord(config map[string]string, record []sdk.Record) (result []sdk.Record, err error) {
	ctx := context.Background()
	tableID := config[googlebigquery.ConfigTableID]
	fmt.Println("*****Record: ", len(record))

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return result, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location: location, // See https://cloud.google.com/bigquery/docs/locations
	}

	// create dataset
	if err := client.Dataset(datasetID).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "duplicate") {
		return result, err
	}
	fmt.Println("Dataset created")
	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return result, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	type sample struct {
		Name string `json:"name"`
		Abb  string `json:"abb"`
	}
	sampleSchema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "abb", Type: bigquery.StringFieldType},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         sampleSchema,
		ExpirationTime: time.Now().AddDate(1, 0, 0), // Table will be automatically deleted in 1 year.
	}
	tableRef := client.Dataset(datasetID).Table(tableID)
	err = tableRef.Create(ctx, metaData)
	fmt.Println("Error: ", err)
	if err != nil && !strings.Contains(err.Error(), "duplicate") {
		return result, err
	}

	var query string

	for i := 0; i < len(record); i++ {
		fmt.Println("length of record  **", len(record))
		name := fmt.Sprintf("name%d", i)
		abb := fmt.Sprintf("name%d", i)
		query = "INSERT INTO `" + projectID + "." + datasetID + "." + tableID + "`  values ('" + name + "' , '" + abb + "')"

		singleRow := sample{Name: name, Abb: abb}
		buffer := &bytes.Buffer{}
		if err = gob.NewEncoder(buffer).Encode(singleRow); err != nil {
			log.Println("error found", err)
			return
		}
		byteKey := buffer.Bytes()

		result = append(result, sdk.Record{Payload: sdk.RawData(byteKey)})
		fmt.Println(query)
		q := client.Query(query)
		q.Location = location

		job, err := q.Run(ctx)
		if err != nil {
			log.Println("Error found: ", err)
		}

		status, err := job.Wait(ctx)
		if err != nil {
			log.Println("Error found: ", err)
			return result, err
		}

		if err = status.Err(); err != nil {
			log.Println("Error found: ", err)
		}
	}
	return result, nil
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

		// fmt.Println(query)
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
		// return fmt.Errorf("bigquery.NewClient: %v", err)
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
