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
	"testing"
)

func TestParseNoConfig(t *testing.T) {
	cfg := map[string]string{}
	_, err := ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got no error")
	}
}

func TestParseSourceConfigAllConfigPresent(t *testing.T) {
	cfg := map[string]string{}
	cfg[KeyServiceAccount] = "test"
	cfg[KeyProjectID] = "test"
	cfg[KeyDatasetID] = "test"
	cfg[KeyLocation] = "test"
	cfg[KeyTableID] = "testTable"
	cfg[KeyPrimaryKeyColName] = "primaryKey"

	_, err := ParseSourceConfig(cfg)
	if err != nil {
		t.Errorf("parse source config, got error %v", err)
	}
}

func TestParseSourceConfigPartialConfig(t *testing.T) {
	cfg := map[string]string{}
	delete(cfg, KeyServiceAccount)
	cfg[KeyProjectID] = "test"
	cfg[KeyDatasetID] = "test"
	cfg[KeyLocation] = "test"

	_, err := ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}

	cfg[KeyServiceAccount] = "test"
	delete(cfg, KeyProjectID)
	cfg[KeyDatasetID] = "test"

	_, err = ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}

	cfg[KeyServiceAccount] = "test"
	cfg[KeyProjectID] = "test"
	delete(cfg, KeyDatasetID)

	_, err = ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}

	cfg[KeyDatasetID] = "test"
	delete(cfg, KeyLocation)

	_, err = ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}
}
