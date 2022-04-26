package googlesource

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	newClient = bigquery.NewClient //(ctx context.Context, projectID string, opts ...option.ClientOption) (*bigquery.Client, error))
)

func (s *Source) ReadGoogleRow(tableID string, position Position, responseCh chan<- sdk.Record, wg *sync.WaitGroup) (err error) {

	lastRow := false
	offset := position.Offset
	defer wg.Done()

	for {
		// Keep on reading till end of table
		sdk.Logger(s.Ctx).Trace().Str("tableID", tableID).Msg("inside read google row infinite for loop")
		if lastRow {
			sdk.Logger(s.Ctx).Trace().Str("tableID", tableID).Msg("Its the last row. Done processing table")
			break
		}

		counter := 0
		//iterator
		it, err := s.runGetRow(offset, tableID)
		if err != nil {
			sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
			return err
		}

		for {
			var row []bigquery.Value
			err := it.Next(&row)

			if err == iterator.Done {
				sdk.Logger(s.Ctx).Trace().Str("counter", fmt.Sprintf("%d", counter)).Msg("iterator is done.")
				if counter < googlebigquery.CounterLimit {
					// if counter is smaller than the limit we has reached the end of iterator. And will break the for loop now.
					lastRow = true
				}
				break
			}
			if err != nil {
				sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("error while iterating")
				return err
			}

			offset = offset + 1
			position := Position{
				TableID: tableID,
				Offset:  offset,
			}

			counter = counter + 1
			recPosition, err := json.Marshal(&position)
			if err != nil {
				sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error marshalling data")
				continue
			}

			// keep the track of last rows fetched for each table.
			// this helps in implementing incremental syncing.
			s.wrtieLatestPosition(position)

			buffer := &bytes.Buffer{}
			if err = gob.NewEncoder(buffer).Encode(row); err != nil {
				sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error in encoding")
				continue
			}
			byteSlice := buffer.Bytes()

			record := sdk.Record{
				CreatedAt: time.Now().UTC(),
				Payload:   sdk.RawData(byteSlice),
				Position:  recPosition}

			responseCh <- record
		}
	}

	return
}

func (s *Source) wrtieLatestPosition(postion Position) {
	s.LatestPositions.lock.Lock()
	s.LatestPositions.LatestPositions[postion.TableID] = postion
	s.LatestPositions.lock.Unlock()
}

// runGetRow sync data for bigquery using bigquery client jobs
func (s *Source) runGetRow(offset int, tableID string) (it *bigquery.RowIterator, err error) {

	q := s.BQReadClient.Query(
		"SELECT * FROM `" + s.SourceConfig.Config.ConfigProjectID + "." + s.SourceConfig.Config.ConfigDatasetID + "." + tableID + "` " +
			"LIMIT " + strconv.Itoa(googlebigquery.CounterLimit) + " OFFSET " + strconv.Itoa(offset))

	sdk.Logger(s.Ctx).Trace().Str("q ", q.Q)
	q.Location = s.SourceConfig.Config.ConfigLocation

	job, err := q.Run(s.tomb.Context(s.Ctx))
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	status, err := job.Wait(s.tomb.Context(s.Ctx))
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	if err := status.Err(); err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	it, err = job.Read(s.tomb.Context(s.Ctx))
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

	client, err := newClient(ctx, projectID, option.WithCredentialsFile(s.SourceConfig.Config.ConfigServiceAccount))
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

	case <-s.tomb.Dead():
		return sdk.Record{}, s.tomb.Err()

	case r := <-s.SDKResponse:
		return r, nil

	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

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

func (s *Source) runIterator() (err error) {

	for {
		select {
		case <-s.tomb.Dying():
			return s.tomb.Err()

		case <-s.ticker.C:
			sdk.Logger(s.Ctx).Error().Msg("ticker started ")
			// create new client everytime the new sync start. This make sure and new tables coming in are handled.
			client, err := newClient(s.tomb.Context(s.Ctx), s.SourceConfig.Config.ConfigProjectID, option.WithCredentialsFile(s.SourceConfig.Config.ConfigServiceAccount))
			if err != nil {
				sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("error found while creating connection. ")
				s.tomb.Kill(err)
				return fmt.Errorf("bigquery.NewClient: %v", err)
			}

			s.BQReadClient = client

			if err = getTables(s); err != nil {
				sdk.Logger(s.Ctx).Trace().Str("err", err.Error()).Msg("error found while fetching tables. Need to stop proccessing ")
				return err
			}

			foundTable := false

			// if its an already running pipeling and we just
			// want to check for any new row. Send the offset as
			// last position where we left.

			if len(s.LatestPositions.LatestPositions) > 0 {
				// wait group make sure that we start new iteration only
				//  after the first iteration is completely done.
				var wg sync.WaitGroup
				for i, tableID := range s.Tables {
					wg.Add(1)
					position := s.LatestPositions.LatestPositions[tableID]

					// s.tomb.Go(func() (err error) {
					// 	fmt.Println("The table position inside go routine: ", tableID, s.Tables[i])
					// 	return s.ReadGoogleRow(s.Tables[i], position, s.SDKResponse, &wg)
					// })

					// TODO: handle error from goroutine
					s.ReadGoogleRow(s.Tables[i], position, s.SDKResponse, &wg)
				}
				wg.Wait()
			} else {

				// if the pipeline has newly started and it was earlier synced.
				// Then we want to skip all the tables which are already synced and
				// pull only after specified position

				var wg sync.WaitGroup
				for _, tableID := range s.Tables {
					wg.Add(1)
					if !foundTable && s.Position.TableID != "" {
						if s.Position.TableID != tableID {
							continue
						} else {
							foundTable = true
						}
					}

					// TODO: handle error from goroutine
					s.ReadGoogleRow(tableID, s.Position, s.SDKResponse, &wg)
					// s.tomb.Go(func() (err error) {
					// 	return s.ReadGoogleRow(tableID, s.Position, s.SDKResponse, &wg)
					// })
				}
				wg.Wait()
			}
		}
	}

}
