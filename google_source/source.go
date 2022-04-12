package googlesource

import (
	"context"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/iterator"
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

		return err
	}

	s.Config = config
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) (err error) {

	flag.Parse()

	if s.Config.Config.ConfigTableID == "" {
		s.Tables, err = listTables(s.Config.Config.ConfigProjectID, s.Config.Config.ConfigDatasetID)
		if err != nil {
			log.Println("we found an error: ", err)
		}
	}

	s.ReadDataFromEndpoint()
	// fmt.Println("end of function: open")
	log.Println("end of function: open")
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {

	log.Println("Start of function: read")

	var response sdk.Record
	var ok bool
	if response, ok = <-s.SDKResponse; !ok {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	log.Println("record found: ", response)

	return response, nil

}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {

	s.BQReadClient.Close()
	return nil
}

// listTables demonstrates iterating through the collection of tables in a given dataset.
func listTables(projectID, datasetID string) ([]string, error) {
	// projectID := "my-project-id"
	// datasetID := "mydataset"
	ctx := context.Background()
	tables := []string{}

	client, err := bigquery.NewClient(ctx, projectID)
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
		// fmt.Printf("Table: %q\n", t.TableID)
	}
	return tables, nil
}
