package googlesource

import (
	"context"
	"fmt"
	"strings"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
)

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := Source{}
	err := con.Configure(context.Background(), make(map[string]string))
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}

	if strings.HasPrefix(err.Error(), "config is invalid:") {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

// func TestConfigureSource_FailsWhenConfigInvalid(t *testing.T) {
// 	con := connector.Source{}
// 	err := con.Configure(context.Background(), map[string]string{"foobar": "foobar"})
// 	if err != nil {
// 		t.Errorf("expected no error, got %v", err)

// 	}

// 	if strings.HasPrefix(err.Error(), "config is missing:") {
// 		t.Errorf("expected error to be about invalid config, got %v", err)
// 	}
// }

func TestTeardownSource_NoOpen(t *testing.T) {
	con := NewSource()
	err := con.Teardown(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}
}

func TestSuccessfulGet(t *testing.T) {

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = "/home/nehagupta/Downloads/conduit-connector-57124175200e.json"
	cfg[googlebigquery.ConfigProjectID] = "conduit-connector"
	cfg[googlebigquery.ConfigDatasetID] = "conduit_1"
	cfg[googlebigquery.ConfigTableID] = "test1"
	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}

	for {
		fmt.Println("Calling read again....")
		// log.Println("Calling read again....")
		record, err := src.Read(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println("*********Record :", record)

	}
}
