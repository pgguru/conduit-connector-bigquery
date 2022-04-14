package googlesource

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
	bqStoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
)

type Source struct {
	sdk.UnimplementedSource
	Session       *bqStoragepb.ReadSession
	BQReadClient  *bqStorage.BigQueryReadClient
	Config        googlebigquery.SourceConfig
	Tables        []string
	Ch            chan *bqStoragepb.AvroRows
	Ctx           context.Context
	ResponseCh    chan *[]string
	ResultCh      chan *[]string
	ErrResponseCh chan error
	SDKResponse   chan sdk.Record
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring a Source Connector...")
	config, err := googlebigquery.ParseSourceConfig(cfg)
	if err != nil {
		sdk.Logger(ctx).Error().Msg("blank config provided")
		return err
	}

	s.Config = config
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) (err error) {

	flag.Parse()

	// ctx = context.Background()
	s.Ctx = ctx
	bqReadClient, err := bqStorage.NewBigQueryReadClient(ctx, option.WithCredentialsFile(s.Config.Config.ConfigServiceAccount))
	if err != nil {
		sdk.Logger(s.Ctx).Info().Str("err", err.Error()).Msg("NewBigQueryStorageClient: ")
		return err
	}
	// defer bqReadClient.Close()
	s.BQReadClient = bqReadClient

	projectID := &s.Config.Config.ConfigProjectID

	// Verify we've been provided a parent project which will contain the read session.  The
	// session may exist in a different project than the table being read.
	if *projectID == "" {
		log.Fatalf("No parent project ID specified, please supply using the --project_id flag.")
	}

	if s.Config.Config.ConfigTableID == "" {
		s.Tables, err = s.listTables(s.Config.Config.ConfigProjectID, s.Config.Config.ConfigDatasetID)
		if err != nil {
			sdk.Logger(ctx).Info().Str("err", err.Error()).Msg("Stated read function")
			log.Println("we found an error: ", err)
		}
	} else {
		s.Tables = strings.SplitAfter(s.Config.Config.ConfigTableID, ",")
	}

	s.SDKResponse = make(chan sdk.Record, 100)
	// TablesCount := len(s.Tables)
	// var lastTable bool
	for _, tableID := range s.Tables {
		// 	if count >= (TablesCount - 1) {
		// 		lastTable = true
		// 	}
		fmt.Println("Get table: ", tableID)
		err = s.ReadDataFromEndpoint(bqReadClient, tableID)
	}

	// close(s.SDKResponse)
	// fmt.Println("end of function: open")
	log.Println("end of function: open")
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {

	sdk.Logger(ctx).Debug().Msg("Stated read function")

	var response sdk.Record
	var ok bool
	time.Sleep(2 * time.Second)

	if len(s.SDKResponse) <= 0 {
		// Sleep so that we wait for first entry to get inserted into response in case of any delay from endpoint.
		time.Sleep(2 * time.Second)
		if len(s.SDKResponse) <= 0 {
			sdk.Logger(ctx).Debug().Msg("no more values in repsonse. closing the channel now.")
			close(s.SDKResponse)
			<-ctx.Done()
			return sdk.Record{}, ctx.Err()
		}
	}
	if response, ok = <-s.SDKResponse; !ok {
		sdk.Logger(ctx).Debug().Msg("no more values in repsonse. closing the channel now.")
		close(s.SDKResponse)
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	return response, nil

}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {

	s.BQReadClient.Close()
	return nil
}
