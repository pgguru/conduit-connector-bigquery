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
	"strings"

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
	bqStoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
)

type Source struct {
	sdk.UnimplementedSource
	Session      *bqStoragepb.ReadSession
	BQReadClient *bqStorage.BigQueryReadClient
	SourceConfig googlebigquery.SourceConfig
	Tables       []string
	AvroRecordCh chan avroRecord
	Ctx          context.Context
	ResponseCh   chan *[]string
	ResultCh     chan *[]string
	SDKResponse  chan sdk.Record
	readStream   string
}

type avroRecord struct {
	avroRow *bqStoragepb.AvroRows
	offset  int
}

type Position struct {
	TableID string
	Offset  int
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Trace().Msg("Configuring a Source Connector.")
	sourceConfig, err := googlebigquery.ParseSourceConfig(cfg)
	if err != nil {
		sdk.Logger(ctx).Error().Msg("blank config provided")
		return err
	}

	s.SourceConfig = sourceConfig
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) (err error) {

	s.Ctx = ctx
	bqReadClient, err := bqStorage.NewBigQueryReadClient(ctx, option.WithCredentialsFile(s.SourceConfig.Config.ConfigServiceAccount))
	if err != nil {
		sdk.Logger(s.Ctx).Trace().Str("err", err.Error()).Msg("error found in NewBigQueryStorageClient client creation ")
		return err
	}

	s.BQReadClient = bqReadClient
	var position Position

	if err = getTables(s); err != nil {
		sdk.Logger(s.Ctx).Trace().Str("err", err.Error()).Msg("error found while fetching tables. Need to stop proccessing ")
		return err
	}

	// if pos != nil {
	position = fetchPos(s, pos)
	// }

	// s.SDKResponse is a buffered channel that contains records coming from all the tables which user wants to sync.
	s.SDKResponse = make(chan sdk.Record, 100)

	foundTable := false

	// Pull records from all the tables and push it to  s.SDKResponse channel
	for _, tableID := range s.Tables {
		// if position is provided we check till we get the table name. And read all the table after the table mentioned in position
		if !foundTable && position.TableID != "" {
			if position.TableID != tableID {
				continue
			} else {
				foundTable = true
			}
		}

		position.TableID = tableID
		err = s.ReadDataFromEndpoint(bqReadClient, tableID, position)
		if err != nil {
			sdk.Logger(ctx).Error().Str("err:", err.Error()).Msg("Error found in reading data")
			return
		}
	}

	sdk.Logger(ctx).Trace().Msg("end of function: open")
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {

	sdk.Logger(ctx).Trace().Msg("Stated read function")
	var response sdk.Record

	if !s.HasNext() {
		sdk.Logger(ctx).Trace().Msg("no more values in repsonse. closing the channel now.")
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	response, err := s.Next(s.Ctx)
	if err != nil {
		sdk.Logger(ctx).Trace().Str("err", err.Error()).Msg("Error from endpoint.")
		return sdk.Record{}, err
	}

	return response, nil

}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {

	if s.SDKResponse != nil {
		close(s.SDKResponse)
	}
	if s.BQReadClient != nil {
		err := s.BQReadClient.Close()
		if err != nil {
			sdk.Logger(ctx).Error().Str("err", string(err.Error())).Msg("got error while closing bigquery client")
			return err
		}
	}
	return nil
}

func fetchPos(s *Source, pos sdk.Position) (position Position) {
	position = Position{TableID: "", Offset: 0}
	err := json.Unmarshal(pos, &position)
	if err != nil {
		sdk.Logger(s.Ctx).Info().Str("err", err.Error()).Msg("Could not get position. Will start with offset 0")
		position = Position{TableID: "", Offset: 0}
		err = nil
	}
	return position
}

func getTables(s *Source) (err error) {
	if s.SourceConfig.Config.ConfigTableID == "" {

		s.Tables, err = s.listTables(s.SourceConfig.Config.ConfigProjectID, s.SourceConfig.Config.ConfigDatasetID)
		if err != nil {
			sdk.Logger(s.Ctx).Trace().Str("err", err.Error()).Msg("error found while listing table")
		}
	} else {
		s.Tables = strings.SplitAfter(s.SourceConfig.Config.ConfigTableID, ",")
	}
	return err
}
