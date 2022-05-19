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
	"fmt"
	"regexp"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"go.uber.org/goleak"
)

// type destination struct {
// 	sdk.UnimplementedDestination
// }

// func NewDestination() sdk.Destination {
// 	return destination{}
// }

// func (d destination) Configure(context.Context, map[string]string) error        { return nil }
// func (d destination) Open(context.Context) error                                { return nil }
// func (d destination) WriteAsync(context.Context, sdk.Record, sdk.AckFunc) error { return nil }
// func (d destination) Write(context.Context, sdk.Record) error                   { return nil }
// func (d destination) Flush(context.Context) error                               { return nil }
// func (d destination) Teardown(context.Context) error                            { return nil }

// func TestAcceptance(t *testing.T) {

// 	cfg := map[string]string{
// 		Servers:           "localhost:9092",
// 		ReadFromBeginning: "true",
// 	}

// 	sdk.AcceptanceTest(t, driver{
// 		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
// 			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
// 				Connector: sdk.Connector{
// 					NewSpecification: Specification,
// 					NewSource:        NewSource,
// 					NewDestination:   NewDestination,
// 				},
// 				SourceConfig:      cfg,
// 				DestinationConfig: cfg,

// 				BeforeTest: func(t *testing.T) {
// 					cfg[Topic] = "TestAcceptance-" + uuid.NewString()
// 				},
// 				AfterTest: func(t *testing.T) {
// 				},
// 				GoleakOptions: []goleak.Option{
// 					// kafka.DefaultTransport starts some goroutines: https://github.com/segmentio/kafka-go/issues/599
// 					goleak.IgnoreTopFunction("github.com/segmentio/kafka-go.(*connPool).discover"),
// 					goleak.IgnoreTopFunction("github.com/segmentio/kafka-go.(*conn).run"),
// 				},
// 			},
// 		},
// 	})
// 	// cfg := map[string]string{
// 	// 	googlebigquery.ConfigServiceAccount:     serviceAccount,
// 	// 	googlebigquery.ConfigProjectID:          projectID,
// 	// 	googlebigquery.ConfigDatasetID:          datasetID,
// 	// 	googlebigquery.ConfigTableID:            tableIDTimeStamp,
// 	// 	googlebigquery.ConfigLocation:           location,
// 	// 	googlebigquery.ConfigIncrementalColName: fmt.Sprintf("%s:updatedat", tableIDTimeStamp),
// 	// 	googlebigquery.ConfigPrimaryKeyColName:  fmt.Sprintf("%s:age", tableIDTimeStamp),
// 	// }

// 	// sdk.AcceptanceTest(t, AcceptanceTestDriver{
// 	// 	Config: AcceptanceSourceTestDriverConfig{
// 	// 		Connector: sdk.Connector{ // Note that this variable should rather be created globally in `connector.go`
// 	// 			NewSpecification: googlebigquery.Specification,
// 	// 			NewSource:        NewSource,
// 	// 			// NewDestination:   NewDestination,
// 	// 		},
// 	// 		SourceConfig: cfg,

// 	// 		BeforeTest: func(t *testing.T) {
// 	// 			DataSetup()
// 	// 			time.Sleep(10 * time.Second)
// 	// 		},
// 	// 	},
// 	// })
// }

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount: serviceAccount,
		googlebigquery.ConfigProjectID:      projectID,
		googlebigquery.ConfigDatasetID:      datasetID,
		googlebigquery.ConfigTableID:        tableID,
		googlebigquery.ConfigLocation:       location,
		// googlebigquery.ConfigIncrementalColName: fmt.Sprintf("%s:updatedat", tableIDTimeStamp),
		// googlebigquery.ConfigPrimaryKeyColName:  fmt.Sprintf("%s:age", tableIDTimeStamp),
	}

	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		Config: AcceptanceSourceTestDriverConfig{
			Connector: sdk.Connector{
				NewSpecification: googlebigquery.Specification,
				NewSource:        NewSource,
				// NewDestination:   NewDestination,
			},
			SourceConfig: cfg,
			// DestinationConfig: cfg,

			// BeforeTest: func(t *testing.T) {
			// 	cfg[Topic] = "TestAcceptance-" + uuid.NewString()
			// },
			// AfterTest: func(t *testing.T) {
			// },
			GoleakOptions: []goleak.Option{
				// kafka.DefaultTransport starts some goroutines: https://github.com/segmentio/kafka-go/issues/599
				// goleak.IgnoreTopFunction("github.com/segmentio/kafka-go.(*connPool).discover"),
				// goleak.IgnoreTopFunction("github.com/segmentio/kafka-go.(*conn).run"),
			},
		},
	},
	)
}

// AcceptanceTestDriver is the default implementation of
// AcceptanceTestDriver. It provides a convenient way of configuring the driver
// without the need of implementing a custom driver from scratch.
type AcceptanceTestDriver struct {
	Config AcceptanceSourceTestDriverConfig
}

// AcceptanceSourceTestDriverConfig contains the configuration for
// AcceptanceTestDriver.
type AcceptanceSourceTestDriverConfig struct {
	// Connector is the connector to be tested.
	Connector sdk.Connector

	// SourceConfig should be a valid config for a source connector, reading
	// from the same location as the destination will write to.
	SourceConfig map[string]string

	// BeforeTest is executed before each acceptance test.
	BeforeTest func(t *testing.T)
	// AfterTest is executed after each acceptance test.
	AfterTest func(t *testing.T)

	// GoleakOptions will be applied to goleak.VerifyNone. Can be used to
	// suppress false positive goroutine leaks.
	GoleakOptions []goleak.Option

	// Skip is a slice of regular expressions used to identify tests that should
	// be skipped. The full test name will be matched against all regular
	// expressions and the test will be skipped if a match is found.
	Skip []string
}

func (d AcceptanceTestDriver) DestinationConfig(*testing.T) map[string]string {
	return map[string]string{}
}
func (d AcceptanceTestDriver) Connector() sdk.Connector {
	return d.Config.Connector
}

func (d AcceptanceTestDriver) SourceConfig(*testing.T) map[string]string {
	return d.Config.SourceConfig
}

func (d AcceptanceTestDriver) BeforeTest(t *testing.T) {
	// before test check if the test should be skipped
	d.Skip(t)

	if d.Config.BeforeTest != nil {
		d.Config.BeforeTest(t)
	}
}

func (d AcceptanceTestDriver) AfterTest(t *testing.T) {
	if d.Config.AfterTest != nil {
		d.Config.AfterTest(t)
	}
}

func (d AcceptanceTestDriver) Skip(t *testing.T) {
	var skipRegexs []*regexp.Regexp
	for _, skipRegex := range d.Config.Skip {
		r := regexp.MustCompile(skipRegex)
		skipRegexs = append(skipRegexs, r)
	}

	for _, skipRegex := range skipRegexs {
		if skipRegex.MatchString(t.Name()) {
			t.Skip(fmt.Sprintf("caller requested to skip tests that match the regex %q", skipRegex.String()))
		}
	}
}

func (d AcceptanceTestDriver) GoleakOptions(_ *testing.T) []goleak.Option {
	return d.Config.GoleakOptions
}

// WriteToSource by default opens the destination and writes records to the
// destination. It is expected that the destination is writing to the same
// location the source is reading from. If the connector does not implement a
// destination the function will fail the test.
func (d AcceptanceTestDriver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
	// if d.Connector().NewDestination == nil {
	// 	t.Fatal("connector is missing the field NewDestination, either implement the destination or overwrite the driver method Write")
	// }

	is := is.New(t)
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	config := d.SourceConfig(t)
	fmt.Println("**config", config)
	err := writeToSource(config)
	is.NoErr(err)
	// d.WriteToSource()

	// // writing something to the destination should result in the same record
	// // being produced by the source
	// dest := d.Connector().NewDestination()
	// err := dest.Configure(ctx, d.DestinationConfig(t))
	// is.NoErr(err)

	// err = dest.Open(ctx)
	// is.NoErr(err)

	// defer func() {
	// 	cancel() // cancel context to simulate stop
	// 	err = dest.Teardown(context.Background())
	// 	is.NoErr(err)
	// }()

	// // try to write using WriteAsync and fallback to Write if it's not supported
	// err := d.writeAsync(ctx, dest, records)
	// if errors.Is(err, sdk.ErrUnimplemented) {
	// err := d.write(ctx, records)
	// // }
	// is.NoErr(err)

	return records
}

func writeToSource(config map[string]string) (err error) {
	err = dataSetup()
	return err
}
func (d AcceptanceTestDriver) ReadFromDestination(*testing.T, []sdk.Record) []sdk.Record {
	return []sdk.Record{}
}

// // write writes records to destination using Destination.Write.
// func (d AcceptanceTestDriver) write(ctx context.Context, records []sdk.Record) error {
// 	for _, r := range records {
// 		err := dest.Write(ctx, r)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	// flush to make sure the records get written to the destination, but allow
// 	// it to be unimplemented
// 	err := dest.Flush(ctx)
// 	if err != nil && !errors.Is(err, sdk.ErrUnimplemented) {
// 		return err
// 	}

// 	// records were successfully written
// 	return nil
// }
