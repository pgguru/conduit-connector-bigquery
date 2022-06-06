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
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "bigquery",
		Summary:     "A BigQuery source plugin for Conduit, written in Go.",
		Description: "A plugin to fetch data from google BigQuery",
		Version:     "v0.1.0",
		Author:      "Neha Gupta",
		SourceParams: map[string]sdk.Parameter{
			ConfigServiceAccount: {
				Default:     "",
				Required:    true,
				Description: "service account key file with data pulling access. ref: https://cloud.google.com/docs/authentication/getting-started", // We can also take it as value if required
			},
			ConfigProjectID: {
				Default:     "",
				Required:    true,
				Description: "Google project ID.",
			},
			ConfigDatasetID: {
				Default:     "",
				Required:    true,
				Description: "Google Bigqueries dataset ID.",
			},
			ConfigLocation: {
				Default:     "",
				Required:    true,
				Description: "Google Bigqueries dataset location.",
			},
			ConfigTableID: {
				Default:     "",
				Required:    true,
				Description: "Google Bigqueries table ID.",
			},
			ConfigPollingTime: {
				Default:     "5",
				Required:    false,
				Description: "polling period for the CDC mode, formatted as a time.Duration string.",
			},
			ConfigIncrementalColName: {
				Default:  "",
				Required: false,
				Description: `Column name which provides visibility about newer rows. For eg, updated_at column which stores when the row was last updated\n
				primary key with incremental value say id of type int or float.  \n eg value,
				 updated_at`,
			},
			ConfigPrimaryKeyColName: {
				Default:  "",
				Required: false,
				Description: `Column name which provides visibility about uniqueness. For eg, _id which stores \n
				primary key with incremental value say id of type int or float.  \n eg value,
				 id`,
			},
		},
	}
}
