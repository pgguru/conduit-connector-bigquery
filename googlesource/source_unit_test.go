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
	"github.com/pgguru/conduit-connector-bigquery/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"gopkg.in/tomb.v2"
)

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := Source{}
	err := con.Configure(context.Background(), make(map[string]string))
	if err == nil {
		t.Errorf("expected no error, got %v", err)
	}

	if strings.HasPrefix(err.Error(), "config is invalid:") {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

func TestSuccessfulTearDown(t *testing.T) {
	if serviceAccount == "" || projectID == "" {
		t.Skip("GOOGLE_SERVICE_ACCOUNT or GOOGLE_PROJECT_ID is missing")
	}

	err := dataSetup(t)
	if err != nil {
		t.Errorf("Could not create values. Err: %v", err)
		return
	}

	defer func() {
		err := cleanupDataset(t, []string{tableID})
		if err != nil {
			t.Log("Got error while cleanup. Err: ", err)
		}
	}()

	src := Source{}
	cfg := map[string]string{}

	cfg[config.KeyServiceAccount] = serviceAccount
	cfg[config.KeyProjectID] = projectID
	cfg[config.KeyDatasetID] = datasetID
	cfg[config.KeyTableID] = tableID
	cfg[config.KeyLocation] = location
	cfg[config.KeyPrimaryKeyColName] = "post_abbr"

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestMultipleTables(t *testing.T) {
	if serviceAccount == "" || projectID == "" {
		t.Skip("GOOGLE_SERVICE_ACCOUNT or GOOGLE_PROJECT_ID is missing")
	}

	err := dataSetup(t)
	if err != nil {
		t.Errorf("Could not create values. Err: %v", err)
		return
	}
	defer func() {
		err := cleanupDataset(t, []string{tableID})
		if err != nil {
			t.Log("Got error while cleanup. Err: ", err)
		}
	}()

	src := Source{}
	cfg := map[string]string{}

	config.PollingTime = time.Second * 1
	cfg[config.KeyServiceAccount] = serviceAccount
	cfg[config.KeyProjectID] = projectID
	cfg[config.KeyDatasetID] = datasetID
	cfg[config.KeyTableID] = tableID
	cfg[config.KeyLocation] = location
	cfg[config.KeyPrimaryKeyColName] = "post_abbr"

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("error found: %v", err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		t.Errorf("error found: %v", err)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestInvalidCreds(t *testing.T) {
	src := Source{}
	cfg := map[string]string{}
	cfg[config.KeyServiceAccount] = "invalid"
	cfg[config.KeyProjectID] = projectID
	cfg[config.KeyDatasetID] = datasetID
	cfg[config.KeyTableID] = tableID
	cfg[config.KeyLocation] = "test"
	cfg[config.KeyPrimaryKeyColName] = "post_abbr"

	config.PollingTime = time.Second * 1
	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err == nil {
		t.Log("we should get error in read")
	}
	time.Sleep(10 * time.Second)
	for {
		_, err := src.Read(ctx)
		if err != nil {
			t.Log("got the expected error. Err: ", err)
			break
		}
		if err == nil {
			t.Errorf("We should have seen some error: %v", err)
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestNewSource(t *testing.T) {
	NewSource()
}

func TestAck(t *testing.T) {
	s := NewSource()
	err := s.Ack(context.TODO(), sdk.Position{})
	if err != nil {
		t.Errorf("Got err: %v", err)
	}
}

func TestNextContextDone(t *testing.T) {
	s := Source{}
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	s.tomb = &tomb.Tomb{}
	_, err := s.Next(ctx)
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

type mockClient struct {
}

func (client *mockClient) Client() (*bigquery.Client, error) {
	return nil, fmt.Errorf("mock error")
}

func TestInvalid(t *testing.T) {
	if serviceAccount == "" || projectID == "" {
		t.Skip("GOOGLE_SERVICE_ACCOUNT or GOOGLE_PROJECT_ID is missing")
	}

	config.PollingTime = time.Second * 1

	src := Source{}
	cfg := map[string]string{}
	cfg[config.KeyServiceAccount] = serviceAccount
	cfg[config.KeyProjectID] = projectID
	cfg[config.KeyDatasetID] = datasetID
	cfg[config.KeyTableID] = tableID
	cfg[config.KeyLocation] = location
	cfg[config.KeyPrimaryKeyColName] = "post_abbr"

	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	pos, err := json.Marshal(fmt.Sprintf("%d", 46))
	if err != nil {
		t.Log(err)
	}
	src.clientType = &mockClient{}
	err = src.Open(ctx, pos)
	if err == nil {
		t.Errorf("expected error, got %v", err)
	}

	time.Sleep(15 * time.Second)
	_, err = src.Read(ctx)
	if err == nil {
		t.Errorf("should have recieved error while pulling data")
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestInvalidOrderByName(t *testing.T) {
	if serviceAccount == "" || projectID == "" {
		t.Skip("GOOGLE_SERVICE_ACCOUNT or GOOGLE_PROJECT_ID is missing")
	}

	err := dataSetup(t)
	if err != nil {
		t.Errorf("Could not create values. Err: %v", err)
		return
	}
	defer func() {
		err := cleanupDataset(t, []string{tableID})
		if err != nil {
			t.Log("Got error while cleanup. Err: ", err)
		}
	}()

	src := Source{}
	cfg := map[string]string{
		config.KeyServiceAccount:     serviceAccount,
		config.KeyProjectID:          projectID,
		config.KeyDatasetID:          datasetID,
		config.KeyLocation:           location,
		config.KeyIncrementalColName: "post_abbr",
		config.KeyPrimaryKeyColName:  "post_abbr",
	}

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err == nil {
		t.Log("expected error. got null")
		t.Errorf("some other error found: %v", err)
	}
}

type mockBQClientStruct struct {
}

func (bq mockBQClientStruct) Query(s *Source, query string) (it rowIterator, err error) {
	return nil, fmt.Errorf("mock error")
}

func (bq mockBQClientStruct) Close() error {
	return fmt.Errorf("mock error")
}

func TestInvalidCloseBQ(t *testing.T) {
	if serviceAccount == "" || projectID == "" {
		t.Skip("GOOGLE_SERVICE_ACCOUNT or GOOGLE_PROJECT_ID is missing")
	}

	config.PollingTime = time.Second * 1

	src := Source{}
	cfg := map[string]string{}
	cfg[config.KeyServiceAccount] = serviceAccount
	cfg[config.KeyProjectID] = projectID
	cfg[config.KeyDatasetID] = datasetID
	cfg[config.KeyTableID] = tableID
	cfg[config.KeyLocation] = location
	cfg[config.KeyPrimaryKeyColName] = "post_abbr"

	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	src.bqReadClient = mockBQClientStruct{}
	src.ctx = ctx

	err = src.Teardown(ctx)
	if err == nil {
		t.Errorf("mock error expected, got %v", err)
	}
}
