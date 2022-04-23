package googlesource

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func (s *Source) ReadGoogleRow(tableID string, position Position, responseCh chan<- sdk.Record) (err error) {

	lastRow := false
	offset := position.Offset

	for {
		if lastRow {
			sdk.Logger(s.Ctx).Debug().Str("tableID", tableID).Msg("Its the last row. Done processing table")
			break
		}

		counter := 0
		//iterator
		it, err := s.runGetRow(offset, tableID)
		if err != nil {
			fmt.Println("Error while running job. Err: ", err)
			sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
			return err
		}

		for {
			var row []bigquery.Value
			err := it.Next(&row)

			if err == iterator.Done {
				if counter < googlebigquery.CounterLimit {
					lastRow = true
				}
				break
			}
			if err != nil {
				sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("error while iterating")
				return err
			}

			position := Position{
				TableID: tableID,
				Offset:  offset,
			}

			offset = offset + 1
			counter = counter + 1

			recPosition, err := json.Marshal(&position)
			if err != nil {
				sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error marshalling data")
				continue
			}

			buffer := &bytes.Buffer{}
			gob.NewEncoder(buffer).Encode(row)
			byteSlice := buffer.Bytes()

			record := sdk.Record{
				CreatedAt: time.Now().UTC(),
				Payload:   sdk.RawData(byteSlice), //sdk.StructuredData TODO:
				Position:  recPosition}

			responseCh <- record
		}
	}

	return
}

func (s *Source) runGetRow(offset int, tableID string) (it *bigquery.RowIterator, err error) {

	q := s.BQReadClient.Query(
		"SELECT * FROM `" + s.SourceConfig.Config.ConfigProjectID + "." + s.SourceConfig.Config.ConfigDatasetID + "." + tableID + "` " +
			"LIMIT " + strconv.Itoa(googlebigquery.CounterLimit) + " OFFSET " + strconv.Itoa(offset))

	sdk.Logger(s.Ctx).Trace().Str("q ", q.Q)
	q.Location = s.SourceConfig.Config.ConfigLocation

	job, err := q.Run(s.Ctx)
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	status, err := job.Wait(s.Ctx)
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	if err := status.Err(); err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	it, err = job.Read(s.Ctx)
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}
	return it, err
}

// listTables demonstrates iterating through the collection of tables in a given dataset.
func (s *Source) listTables(projectID, datasetID string) ([]string, error) {
	ctx := context.Background()
	tables := []string{}

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(s.SourceConfig.Config.ConfigServiceAccount))
	if err != nil {
		return []string{}, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	ts := client.Dataset(datasetID).Tables(ctx)
	for {
		t, err := ts.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return []string{}, err
		}
		tables = append(tables, t.TableID)
	}
	return tables, nil
}

// Next returns the next record from the buffer.
func (s *Source) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case r := <-s.SDKResponse:
		return r, nil
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

func (s *Source) HasNext() bool {
	if len(s.SDKResponse) <= 0 {
		sdk.Logger(s.Ctx).Debug().Msg("We will try in 2 seconds.")
		time.Sleep(2 * time.Second)
		if len(s.SDKResponse) <= 0 {
			sdk.Logger(s.Ctx).Trace().Msg("We are done with pulling info")
			return false
		}
	}
	return true
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
