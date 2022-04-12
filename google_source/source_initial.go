// The bigquery_storage_quickstart application demonstrates usage of the
// BigQuery Storage read API.  It demonstrates API features such as column
// projection (limiting the output to a subset of a table's columns),
// column filtering (using simple predicates to filter records on the server
// side), establishing the snapshot time (reading data from the table at a
// specific point in time), and decoding Avro row blocks using the third party
// "github.com/linkedin/goavro" library.
package googlesource

import (
	"flag"
	"fmt"
	"log"
	"sort"

	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc"
)

// rpcOpts is used to configure the underlying gRPC client to accept large
// messages.  The BigQuery Storage API may send message blocks up to 128MB
// in size.
var rpcOpts = gax.WithGRPCOptions(
	grpc.MaxCallRecvMsgSize(1024 * 1024 * 129),
)

// Command-line flags.
var (
	// projectID = flag.String("project_id", "conduit-connector",
	// 	"Cloud Project ID, used for session creation.")
	snapshotMillis = flag.Int64("snapshot_millis", 0,
		"Snapshot time to use for reads, represented in epoch milliseconds format.  Default behavior reads current data.")
)

// func Main1() {
// 	flag.Parse()
// 	ctx := context.Background()
// 	bqReadClient, err := bqStorage.NewBigQueryReadClient(ctx)
// 	if err != nil {
// 		log.Fatalf("NewBigQueryStorageClient: %v", err)
// 	}
// 	defer bqReadClient.Close()

// 	// Verify we've been provided a parent project which will contain the read session.  The
// 	// session may exist in a different project than the table being read.
// 	if *projectID == "" {
// 		log.Fatalf("No parent project ID specified, please supply using the --project_id flag.")
// 	}

// 	// This example uses baby name data from the public datasets.
// 	srcProjectID := "conduit-connector"
// 	srcDatasetID := "conduit_1"
// 	srcTableID := "test1"
// 	readTable := fmt.Sprintf("projects/%s/datasets/%s/tables/%s",
// 		srcProjectID,
// 		srcDatasetID,
// 		srcTableID,
// 	)

// 	// We limit the output columns to a subset of those allowed in the table,
// 	// and set a simple filter to only report names from the state of
// 	// Washington (WA).
// 	tableReadOptions := &bqStoragepb.ReadSession_TableReadOptions{
// 		SelectedFields: []string{"Name", "phoneNo", "id"},
// 		// RowRestriction: `state = "WA"`,
// 	}

// 	createReadSessionRequest := &bqStoragepb.CreateReadSessionRequest{
// 		Parent: fmt.Sprintf("projects/%s", *projectID),
// 		ReadSession: &bqStoragepb.ReadSession{
// 			Table: readTable,
// 			// This API can also deliver data serialized in Apache Arrow format.
// 			// This example leverages Apache Avro.
// 			DataFormat:  bqStoragepb.DataFormat_AVRO,
// 			ReadOptions: tableReadOptions,
// 		},
// 		MaxStreamCount: 1,
// 	}

// 	// Set a snapshot time if it's been specified.
// 	if *snapshotMillis > 0 {
// 		ts, err := ptypes.TimestampProto(time.Unix(0, *snapshotMillis*1000))
// 		if err != nil {
// 			log.Fatalf("Invalid snapshot millis (%d): %v", *snapshotMillis, err)
// 		}
// 		createReadSessionRequest.ReadSession.TableModifiers = &bqStoragepb.ReadSession_TableModifiers{
// 			SnapshotTime: ts,
// 		}
// 	}

// 	// Create the session from the request.
// 	session, err := bqReadClient.CreateReadSession(ctx, createReadSessionRequest, rpcOpts)
// 	if err != nil {
// 		log.Fatalf("CreateReadSession: %v", err)
// 	}
// 	fmt.Printf("Read session: %s\n", session.GetName())

// 	if len(session.GetStreams()) == 0 {
// 		log.Fatalf("no streams in session.  if this was a small query result, consider writing to output to a named table.")
// 	}

// 	// We'll use only a single stream for reading data from the table.  Because
// 	// of dynamic sharding, this will yield all the rows in the table. However,
// 	// if you wanted to fan out multiple readers you could do so by having a
// 	// increasing the MaxStreamCount.
// 	readStream := session.GetStreams()[0].Name

// 	ch := make(chan *bqStoragepb.AvroRows)

// 	// Use a waitgroup to coordinate the reading and decoding goroutines.
// 	var wg sync.WaitGroup

// 	// Start the reading in one goroutine.
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		if err := processStream(ctx, bqReadClient, readStream, ch); err != nil {
// 			log.Fatalf("processStream failure: %v", err)
// 		}
// 		close(ch)
// 	}()

// 	// Start Avro processing and decoding in another goroutine.
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		err := processAvro(ctx, session.GetAvroSchema().GetSchema(), ch)
// 		if err != nil {
// 			log.Fatalf("Error processing avro: %v", err)
// 		}
// 	}()

// 	// Wait until both the reading and decoding goroutines complete.
// 	wg.Wait()

// }

// printDatum prints the decoded row datum.
func printDatum(d interface{}) (response []string) {
	log.Println("Came here")
	m, ok := d.(map[string]interface{})
	if !ok {
		log.Printf("failed type assertion: %v", d)
	}
	// Go's map implementation returns keys in a random ordering, so we sort
	// the keys before accessing.
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		response = append(response, fmt.Sprintf("%s: %-20v \n", key, valueFromTypeMap(m[key])))
		// response = append(response, fmt.Println())
	}

	return response
}

// valueFromTypeMap returns the first value/key in the type map.  This function
// is only suitable for simple schemas, as complex typing such as arrays and
// records necessitate a more robust implementation.  See the goavro library
// and the Avro specification for more information.
func valueFromTypeMap(field interface{}) interface{} {
	m, ok := field.(map[string]interface{})
	if !ok {
		return nil
	}
	for _, v := range m {
		// Return the first key encountered.
		return v
	}
	return nil
}
