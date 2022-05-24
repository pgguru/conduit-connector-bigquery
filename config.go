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
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	// ConfigProjectID is the config projectID
	ConfigProjectID = "projectID"

	// ConfigDatasetID is the dataset ID
	ConfigDatasetID = "datasetID"

	// ConfigTableID is the tableID
	ConfigTableID = "tableID"

	// ConfigServiceAccount path to service account key
	ConfigServiceAccount = "serviceAccount"

	// ConfigLocation location of the dataset
	ConfigLocation = "datasetLocation"

	// ConfigPollingTime time after which polling should be done
	ConfigPollingTime = "pollingTime"

	// ConfigOrderBy lets user decide
	ConfigIncrementalColName = "incrementingColumnName"

	// ConfigPrimaryKeyColName provide primary key
	ConfigPrimaryKeyColName = "primaryKeyColName"
)

// Config represents configuration needed for S3
type Config struct {
	ProjectID         string
	DatasetID         string
	TableIDs          string
	ServiceAccount    string
	Location          string
	PollingTime       string
	IncrementColName  map[string]string // IncrementColName is map of tableID and corresponding column which would be used as incrementing column
	PrimaryKeyColName map[string]string
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

func ParseSourceConfig(cfg map[string]string) (SourceConfig, error) {
	err := checkEmpty(cfg)
	if err != nil {
		log.Println("Empty config found:", err)
		return SourceConfig{}, err
	}

	if _, ok := cfg[ConfigServiceAccount]; !ok {
		return SourceConfig{}, errors.New("service account can't be blank")
	}

	if _, ok := cfg[ConfigProjectID]; !ok {
		return SourceConfig{}, errors.New("project ID can't be blank")
	}

	if _, ok := cfg[ConfigDatasetID]; !ok {
		return SourceConfig{}, errors.New("dataset ID can't be blank")
	}

	if _, ok := cfg[ConfigLocation]; !ok {
		return SourceConfig{}, errors.New("location can't be blank")
	}

	config := Config{
		ServiceAccount: cfg[ConfigServiceAccount],
		ProjectID:      cfg[ConfigProjectID],
		DatasetID:      cfg[ConfigDatasetID],
		TableIDs:       cfg[ConfigTableID],
		Location:       cfg[ConfigLocation],
		PollingTime:    cfg[ConfigPollingTime]}

	if orderby, ok := cfg[ConfigIncrementalColName]; ok {
		config.IncrementColName = make(map[string]string)
		tableArr := strings.Split(orderby, ",")
		for _, table := range tableArr {
			singleTableRow := strings.Split(table, ":")
			if len(singleTableRow) < 2 {
				return SourceConfig{}, errors.New("invalid formating of incremental column, should be tableid:columnName,anotherTableid:anotherColumnName")
			}
			config.IncrementColName[singleTableRow[0]] = singleTableRow[1]
		}
	}

	if primaryKey, ok := cfg[ConfigPrimaryKeyColName]; ok {
		config.PrimaryKeyColName = make(map[string]string)
		tableArr := strings.Split(primaryKey, ",")
		for _, table := range tableArr {
			singleTableRow := strings.Split(table, ":")
			if len(singleTableRow) < 2 {
				return SourceConfig{}, errors.New("invalid formating of primary key column, should be tableid:columnName,anotherTableid:anotherColumnName")
			}
			config.PrimaryKeyColName[singleTableRow[0]] = singleTableRow[1]
		}
	}

	return SourceConfig{
		Config: config,
	}, nil
}

func checkEmpty(cfg map[string]string) error {
	if len(cfg) == 0 {
		return fmt.Errorf("empty config found")
	}
	return nil
}
