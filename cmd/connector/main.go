package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	connector "github.com/neha-Gupta1/conduit-connector-bigquery"
	googlesource "github.com/neha-Gupta1/conduit-connector-bigquery/google_source"
	//  bigquery	github.com/neha-Gupta1/conduit-connector-bigquery
)

func main() {
	sdk.Serve(connector.Specification, googlesource.NewSource, nil)
	// connector.Main1()
}
