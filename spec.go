package googlebigquery

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "bigquery",
		Summary:     "A bigquery source plugin for Conduit, written in Go.",
		Description: "A plugin to fetch data from google bigquery",
		Version:     "v0.1.0",
		Author:      "Infracloud",
		SourceParams: map[string]sdk.Parameter{
			ConfigServiceAccount: {
				Default:     "",
				Required:    true,
				Description: "Path to service account key with data pulling access.",
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
			ConfigTableID: {
				Default:     "",
				Required:    false,
				Description: "Google Bigqueries table ID. Can provide `,` separated ID. Will pull whole dataset if no tableID provided.",
			},
		},
	}
}
