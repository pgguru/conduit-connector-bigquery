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
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/pgguru/conduit-connector-bigquery/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// clientFactory provides function to create BigQuery Client
type clientFactory interface {
	Client() (*bigquery.Client, error)
}

type client struct {
	ctx       context.Context
	projectID string
	opts      []option.ClientOption
}

func (client *client) Client() (*bigquery.Client, error) {
	return bigquery.NewClient(client.ctx, client.projectID, client.opts...)
}

type bqClient interface {
	Query(s *Source, query string) (it rowIterator, err error)
	Close() error
}

type bqClientStruct struct {
	client *bigquery.Client
}

func (bq bqClientStruct) Query(s *Source, query string) (it rowIterator, err error) {
	ctx := s.ctx
	q := bq.client.Query(query)
	sdk.Logger(ctx).Trace().Str("q ", q.Q)
	q.Location = s.sourceConfig.Config.Location

	job, err := q.Run(ctx)
	if err != nil {
		sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("Error while running the job")
		return it, err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	if err := status.Err(); err != nil {
		sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	bqIter, err := job.Read(ctx)
	if err != nil {
		sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}
	it = rowIter{it: bqIter}
	return
}

func (bq bqClientStruct) Close() error {
	return bq.client.Close()
}

// checkInitialPos helps in creating the query to fetch data from endpoint
func (s *Source) checkInitialPos() (firstSync, userDefinedOffset, userDefinedKey bool) {
	// if its the firstSync no offset is applied
	if s.getPosition() == "" {
		firstSync = true
	}

	// if incrementColName set - we orderBy the provided column name
	if len(s.sourceConfig.Config.IncrementColName) > 0 {
		userDefinedOffset = true
	}

	// if primaryColName set - we orderBy the provided column name
	if len(s.sourceConfig.Config.PrimaryKeyColName) > 0 {
		userDefinedKey = true
	}

	return
}

func (s *Source) getPosition() string {
	s.position.lock.Lock()
	defer s.position.lock.Unlock()
	return s.position.positions
}

// ReadGoogleRow fetches data from endpoint. It creates sdk.record and puts it in response channel
func (s *Source) ReadGoogleRow(ctx context.Context) (err error) {
	sdk.Logger(ctx).Trace().Msg("Inside read google row")
	var userDefinedOffset, userDefinedKey, firstSync bool

	offset := s.getPosition()
	tableID := s.sourceConfig.Config.TableID

	firstSync, userDefinedOffset, userDefinedKey = s.checkInitialPos()
	lastRow := false

	for {
		// Keep on reading till end of table
		sdk.Logger(ctx).Trace().Str("tableID", tableID).Msg("inside read google row infinite for loop")
		if lastRow {
			sdk.Logger(ctx).Trace().Str("tableID", tableID).Msg("Its the last row. Done processing table")
			break
		}

		counter := 0
		// iterator
		it, err := s.getRowIterator(ctx, offset, tableID, firstSync)
		if err != nil {
			sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("Error while running job")
			return err
		}

		for {
			var row []bigquery.Value

			err := it.Next(&row)
			schema := it.Schema()

			if err == iterator.Done {
				sdk.Logger(ctx).Trace().Str("counter", fmt.Sprintf("%d", counter)).Msg("iterator is done.")
				if counter < config.CounterLimit {
					// if counter is smaller than the limit we have reached the end of
					// iterator. And will break the for loop now.
					lastRow = true
				}
				break
			}
			if err != nil {
				sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("error while iterating")
				return err
			}

			data := make(sdk.StructuredData)
			var key string

			for i, r := range row {
				// handle dates
				if schema[i].Type == bigquery.TimestampFieldType {
					dateR := fmt.Sprintf("%v", r)
					dateLocal, err := time.Parse("2006-01-02 15:04:05.999999 -0700 MST", dateR)
					if err != nil {
						sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("Error while converting to time format")
						return err
					}
					r = dateLocal.Format("2006-01-02 15:04:05.999999 MST")
				}
				data[schema[i].Name] = r

				// if we have found the user provided incremental key that would be used as offset
				if userDefinedOffset {
					if schema[i].Name == s.sourceConfig.Config.IncrementColName {
						offset = fmt.Sprint(data[schema[i].Name])
						offset = getType(schema[i].Type, offset)
					}
				} else {
					offset, err = calcOffset(firstSync, offset)
					if err != nil {
						sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("Error marshalling key")
						continue
					}
				}

				// if we have found the user provided incremental key that would be used as offset
				if userDefinedKey {
					key = matchColumnName(schema[i].Name, s.sourceConfig.Config.PrimaryKeyColName, data)
				}
			}

			buffer := &bytes.Buffer{}
			if err := gob.NewEncoder(buffer).Encode(key); err != nil {
				sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("Error marshalling key")
				continue
			}
			byteKey := buffer.Bytes()

			counter++
			firstSync = false

			// keep the track of last rows fetched for each table.
			// this helps in implementing incremental syncing.
			recPosition, err := s.writePosition(offset)
			if err != nil {
				sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("Error marshalling data")
				continue
			}

			metadata := make(sdk.Metadata)
			metadata.SetCreatedAt(time.Now().UTC())

			record := sdk.Util.Source.NewRecordCreate(recPosition, metadata, sdk.RawData(byteKey), data)

			// select statement to make sure channel was not closed by teardown stage
			if s.iteratorClosed {
				sdk.Logger(ctx).Trace().Msg("recieved closed channel")
				return nil
			}
			s.records <- record
		}
	}
	return
}

// matchColumnName matches if the column name is equal to the user defined primary key column.
// if it is so assign this column name data to key for the record
func matchColumnName(dataName, columnName string, data sdk.StructuredData) (key string) {
	if dataName == columnName {
		key = fmt.Sprintf("%v", data[dataName])
	}
	return key
}

func calcOffset(firstSync bool, offset string) (string, error) {
	// if user doesn't provide any incremental key we manually create offsets to pull data
	if firstSync {
		offset = "0"
	}
	offsetInt, err := strconv.Atoi(offset)
	if err != nil {
		return offset, err
	}
	offsetInt++
	offset = fmt.Sprintf("%d", offsetInt)
	return offset, err
}

func getType(fieldType bigquery.FieldType, offset string) string {
	switch fieldType {
	case bigquery.IntegerFieldType:
		return offset
	case bigquery.FloatFieldType:
		return offset
	case bigquery.NumericFieldType:
		return offset
	case bigquery.BigNumericFieldType:
		return offset
	case bigquery.TimeFieldType:
		return fmt.Sprintf("'%s'", offset)

	default:
		return fmt.Sprintf("'%s'", offset)
	}
}

// writePosition prevents race condition happening while using map inside goroutine
func (s *Source) writePosition(offset string) (recPosition []byte, err error) {
	s.position.lock.Lock()
	defer s.position.lock.Unlock()
	s.position.positions = offset
	return json.Marshal(&s.position.positions)
}

type rowIter struct {
	it *bigquery.RowIterator
}
type rowIterator interface {
	Next(dst interface{}) error
	Schema() bigquery.Schema
}

func (iterator rowIter) Schema() bigquery.Schema {
	return iterator.it.Schema
}

func (iterator rowIter) Next(dst interface{}) error {
	return iterator.it.Next(dst)
}

// getRowIterator sync data for bigquery using bigquery client jobs
func (s *Source) getRowIterator(ctx context.Context, offset string, tableID string, firstSync bool) (it rowIterator, err error) {
	// check for config `IncrementColNames`. User can provide the column name which
	// would be used as orderBy as well as incremental or offset value. Orderby is not mandatory though

	var query string
	if len(s.sourceConfig.Config.IncrementColName) > 0 {
		columnName := s.sourceConfig.Config.IncrementColName
		if firstSync {
			query = "SELECT * FROM `" + s.sourceConfig.Config.ProjectID + "." + s.sourceConfig.Config.DatasetID + "." + tableID + "` " +
				" ORDER BY " + columnName + " LIMIT " + strconv.Itoa(config.CounterLimit)
		} else {
			query = "SELECT * FROM `" + s.sourceConfig.Config.ProjectID + "." + s.sourceConfig.Config.DatasetID + "." + tableID + "` WHERE " + columnName +
				" > " + offset + " ORDER BY " + columnName + " LIMIT " + strconv.Itoa(config.CounterLimit)
		}
	} else {
		// add default value if none specified
		if len(offset) == 0 {
			offset = "0"
		}
		// if no incremental value provided using default offset which is created by incrementing a counter each time a row is sync.
		query = "SELECT * FROM `" + s.sourceConfig.Config.ProjectID + "." + s.sourceConfig.Config.DatasetID + "." + tableID + "` " +
			" LIMIT " + strconv.Itoa(config.CounterLimit) + " OFFSET " + offset
	}

	return s.bqReadClient.Query(s, query)
}

// Next returns the next record from the buffer.
func (s *Source) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-s.tomb.Dead():
		return sdk.Record{}, s.tomb.Err()
	case r := <-s.records:
		return r, nil
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
}

// fetchPos unmarshal position
func fetchPos(s *Source, pos sdk.Position) {
	s.position.lock = new(sync.Mutex)
	s.position.lock.Lock()
	defer s.position.lock.Unlock()
	s.position.positions = ""

	err := json.Unmarshal(pos, &s.position.positions)
	if err != nil {
		sdk.Logger(s.ctx).Info().Msg("Could not get position. Will start with offset 0")
	}
}

func (s *Source) runIterator() (err error) {
	// Snapshot sync. Start were we left last
	ctx := s.ctx
	err = s.ReadGoogleRow(ctx)
	if err != nil {
		sdk.Logger(ctx).Trace().Str("err", err.Error()).Msg("error found while reading google row.")
		return err
	}

	for {
		select {
		case <-s.tomb.Dying():
			return s.tomb.Err()
		case <-s.ticker.C:
			sdk.Logger(ctx).Trace().Msg("ticker started ")
			err = s.ReadGoogleRow(ctx)
			if err != nil {
				sdk.Logger(ctx).Trace().Msg(fmt.Sprintf("error found %v", err))
				return
			}
		}
	}
}
