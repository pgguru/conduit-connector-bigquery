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
	"errors"
	"fmt"
	"time"

	"github.com/pgguru/conduit-connector-bigquery/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/option"
	"gopkg.in/tomb.v2"
)

type Destination struct {
	sdk.UnimplementedDestination
	bqWriteClient bqClient
	destinationConfig config.DestinationConfig
	// for all the function running in goroutine we needed the ctx value. To provide the current
	// ctx value ctx was required in struct.
	ctx            context.Context
	records        chan sdk.Record
	position       position
	ticker         *time.Ticker
	tomb           *tomb.Tomb
	iteratorClosed bool
	// interface to provide BigQuery client. In testing this will be used to mock the client
	clientType clientFactory
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyServiceAccount: {
			Default:     "",
			Required:    true,
			Description: "service account key file with data pulling access. ref: https://cloud.google.com/docs/authentication/getting-started", // We can also take it as value if required
		},
		config.KeyProjectID: {
			Default:     "",
			Required:    true,
			Description: "Google project ID.",
		},
		config.KeyDatasetID: {
			Default:     "",
			Required:    true,
			Description: "Google Bigqueries dataset ID.",
		},
		config.KeyLocation: {
			Default:     "",
			Required:    true,
			Description: "Google Bigqueries dataset location.",
		},
		config.KeyTableID: {
			Default:     "",
			Required:    true,
			Description: "Google Bigqueries table ID.",
		},
		config.KeyPollingTime: {
			Default:     "5ms",
			Required:    false,
			Description: "polling period for the CDC mode, formatted as a time.Duration string.",
		},
		config.KeyIncrementalColName: {
			Default:  "",
			Required: false,
			Description: `Column name which provides visibility about newer rows. For eg, updated_at column which stores when the row was last updated\n
			primary key with incremental value say id of type int or float.  \n eg value,
			 updated_at`,
		},
		config.KeyPrimaryKeyColName: {
			Default:  "",
			Required: false,
			Description: `Column name which provides visibility about uniqueness. For eg, _id which stores \n
			primary key with incremental value say id of type int or float.  \n eg value,
			 id`,
		},
	}
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Trace().Msg("Configuring a Destination Connector.")
	destinationConfig, err := config.ParseDestinationConfig(cfg)
	if err != nil {
		sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("invalid config provided")
		return err
	}

	d.destinationConfig = destinationConfig
	d.clientType = &client{
		ctx:       ctx,
		projectID: d.destinationConfig.Config.ProjectID,
		opts: []option.ClientOption{
			option.WithCredentialsJSON([]byte(d.destinationConfig.Config.ServiceAccount)),
		},
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) (err error) {
	d.ctx = ctx

	pollingTime := config.PollingTime

	// d.records is a buffered channel that contains records
	//  coming from all the tables which user wants to sync.
	d.records = make(chan sdk.Record, 100)
	d.iteratorClosed = false

	if len(d.destinationConfig.Config.PollingTime) > 0 {
		pollingTime, err = time.ParseDuration(d.destinationConfig.Config.PollingTime)
		if err != nil {
			sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("error found while getting time.")
			return errors.New("invalid polling time duration provided")
		}
	}

	d.ticker = time.NewTicker(pollingTime)
	d.tomb = &tomb.Tomb{}
	client, err := d.clientType.Client()
	if err != nil {
		sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("error found while creating connection. ")
		clientErr := fmt.Errorf("error while creating bigquery client: %s", err.Error())
		return clientErr
	}
	bqClient := bqClientStruct{client: client}
	d.bqWriteClient = bqClient

	sdk.Logger(ctx).Trace().Msg("end of function: open")
	return nil
}

func (d *Destination) Write(ctx context.Context, r []sdk.Record) (int, error) {
	sdk.Logger(ctx).Trace().Msg("Started write function")

	return len(r), nil
	
  // 	return sdk.Util.Route(ctx, r,
  //   d.handleInsert,
  //   d.handleUpdate,
  //   d.handleDelete,
  //   d.handleSnapshot, // we could also reuse d.handleInsert
  // )
}

func (d *Destination) handleInsert(ctx context.Context, r sdk.Record) error {
	sdk.Logger(ctx).Trace().Msg("INSERT into bigquery")
	return nil
}

func (d *Destination) handleUpdate(ctx context.Context, r sdk.Record) error {
	sdk.Logger(ctx).Trace().Msg("UPDATE bigquery")
	return nil
}

func (d *Destination) handleDelete(ctx context.Context, r sdk.Record) error {
	sdk.Logger(ctx).Trace().Msg("DELETE from bigquery")
	return nil
}

func (d *Destination) handleSnapshot(ctx context.Context, r sdk.Record) error {
	sdk.Logger(ctx).Trace().Msg("refresh snapshot for bigquery")
	return nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	d.iteratorClosed = true

	if d.records != nil {
		close(d.records)
	}
	err := d.StopIterator()
	if err != nil {
		sdk.Logger(d.ctx).Error().Str("err", err.Error()).Msg("got error while closing BigQuery client")
		return err
	}
	return nil
}

func (d *Destination) StopIterator() error {
	d.iteratorClosed = true
	if d.bqWriteClient != nil {
		err := d.bqWriteClient.Close()
		if err != nil {
			sdk.Logger(d.ctx).Error().Str("err", err.Error()).Msg("got error while closing BigQuery client")
			return err
		}
	}
	if d.ticker != nil {
		d.ticker.Stop()
	}
	if d.tomb != nil {
		d.tomb.Kill(errors.New("iterator is stopped"))
	}
	return nil
}
