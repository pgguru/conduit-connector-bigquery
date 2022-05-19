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
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
)

func TestSuccessTimeIncremental(t *testing.T) {
	// cleanupDataset([]string{tableIDTimeStamp})
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
	// cleanupDataset([]string{tableIDTimeStamp})
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
	// cleanupDataset([]string{tableIDTimeStamp})
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
	// cleanupDataSet()
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
	// cleanupDataSet()
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
	// cleanupDataSet()
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
