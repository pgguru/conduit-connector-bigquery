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

package config

import (
	"errors"
	"fmt"
	"log"
	"time"
)

const (
	// KeyProjectID is the config projectID
	KeyProjectID = "projectID"

	// KeyDatasetID is the dataset ID
	KeyDatasetID = "datasetID"

	// KeyTableID is the tableID
	KeyTableID = "tableID"

	// KeyServiceAccount path to service account key
	KeyServiceAccount = "serviceAccount"

	// KeyLocation location of the dataset
	KeyLocation = "datasetLocation"

	// KeyPollingTime time after which polling should be done
	KeyPollingTime = "pollingTime"

	// ConfigOrderBy lets user decide
	KeyIncrementalColName = "incrementingColumnName"

	// KeyPrimaryKeyColName provide primary key
	KeyPrimaryKeyColName = "primaryKeyColName"
)

// Config represents configuration needed for S3
type Config struct {
	ProjectID         string
	DatasetID         string
	TableID           string
	ServiceAccount    string
	Location          string
	PollingTime       string
	IncrementColName  string // IncrementColName is incrementing column name. This is used as offset
	PrimaryKeyColName string // PrimaryKeyColName is primary key column. This is used as primary key
}

var (
	// CounterLimit sets limit of how many rows will be fetched in each job
	CounterLimit = 500
	PollingTime  = time.Minute * 5
	TimeoutTime  = time.Second * 120
)

// SourceConfig is config for source
type SourceConfig struct {
	Config Config
}

// DestinationConfig is config for destination
type DestinationConfig struct {
	Config Config
}

func ParseSourceConfig(cfg map[string]string) (SourceConfig, error) {
	err := checkEmpty(cfg)
	if err != nil {
		log.Println("Empty config found:", err)
		return SourceConfig{}, err
	}

	if _, ok := cfg[KeyServiceAccount]; !ok {
		return SourceConfig{}, errors.New("service account can't be blank")
	}

	if _, ok := cfg[KeyProjectID]; !ok {
		return SourceConfig{}, errors.New("project ID can't be blank")
	}

	if _, ok := cfg[KeyDatasetID]; !ok {
		return SourceConfig{}, errors.New("dataset ID can't be blank")
	}

	if _, ok := cfg[KeyLocation]; !ok {
		return SourceConfig{}, errors.New("location can't be blank")
	}

	if _, ok := cfg[KeyTableID]; !ok {
		return SourceConfig{}, errors.New("tableID can't be blank")
	}

	if _, ok := cfg[KeyPrimaryKeyColName]; !ok {
		return SourceConfig{}, errors.New("primary key can't be blank")
	}

	config := Config{
		ServiceAccount:    cfg[KeyServiceAccount],
		ProjectID:         cfg[KeyProjectID],
		DatasetID:         cfg[KeyDatasetID],
		TableID:           cfg[KeyTableID],
		Location:          cfg[KeyLocation],
		PollingTime:       cfg[KeyPollingTime],
		IncrementColName:  cfg[KeyIncrementalColName],
		PrimaryKeyColName: cfg[KeyPrimaryKeyColName]}

	return SourceConfig{
		Config: config,
	}, nil
}

func ParseDestinationConfig(cfg map[string]string) (DestinationConfig, error) {
	err := checkEmpty(cfg)
	if err != nil {
		log.Println("Empty config found:", err)
		return DestinationConfig{}, err
	}

	if _, ok := cfg[KeyServiceAccount]; !ok {
		return DestinationConfig{}, errors.New("service account can't be blank")
	}

	if _, ok := cfg[KeyProjectID]; !ok {
		return DestinationConfig{}, errors.New("project ID can't be blank")
	}

	if _, ok := cfg[KeyDatasetID]; !ok {
		return DestinationConfig{}, errors.New("dataset ID can't be blank")
	}

	if _, ok := cfg[KeyLocation]; !ok {
		return DestinationConfig{}, errors.New("location can't be blank")
	}

	if _, ok := cfg[KeyTableID]; !ok {
		return DestinationConfig{}, errors.New("tableID can't be blank")
	}

	if _, ok := cfg[KeyPrimaryKeyColName]; !ok {
		return DestinationConfig{}, errors.New("primary key can't be blank")
	}

	config := Config{
		ServiceAccount:    cfg[KeyServiceAccount],
		ProjectID:         cfg[KeyProjectID],
		DatasetID:         cfg[KeyDatasetID],
		TableID:           cfg[KeyTableID],
		Location:          cfg[KeyLocation],
		PollingTime:       cfg[KeyPollingTime],
		IncrementColName:  cfg[KeyIncrementalColName],
		PrimaryKeyColName: cfg[KeyPrimaryKeyColName]}

	return DestinationConfig{
		Config: config,
	}, nil
}

func checkEmpty(cfg map[string]string) error {
	if len(cfg) == 0 {
		return fmt.Errorf("empty config found")
	}
	return nil
}
