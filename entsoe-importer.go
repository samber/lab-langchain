package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sync/atomic"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/gocarina/gocsv"
	"github.com/sirupsen/logrus"
)

type CSVLine struct {
	Region       string    `csv:"region"`
	Variable     string    `csv:"variable"`
	Attribute    string    `csv:"attribute"`
	UTCTimestamp time.Time `csv:"utc_timestamp"`
	Value        float32   `csv:"data"`
}

func hash(input string) (string, error) {
	h := sha256.New()

	_, err := h.Write([]byte(input))
	if err != nil {
		return "", err
	}

	b := h.Sum(nil)
	return fmt.Sprintf("%x", b), nil
}

func main() {
	fmt.Println("Reading CSV")

	csvFile, err := os.Open("time_series_60min_stacked.csv")
	if err != nil {
		logrus.Fatal(err.Error())
	}

	lines := []CSVLine{}

	err = gocsv.UnmarshalFile(csvFile, &lines)
	if err != nil {
		logrus.Fatal(err.Error())
	}

	start := time.Now()

	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://51.159.148.147:9200",
			// "http://localhost:9200",
		},
		Username: "elastic",
		Password: "screeb",
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error connecting elasticsearch: %s", err)
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         "entsoe",         // The default index name
		Client:        es,               // The Elasticsearch client
		NumWorkers:    10,               // The number of worker goroutines
		FlushBytes:    int(1024 * 1024), // The flush threshold in bytes
		FlushInterval: 30 * time.Second, // The periodic flush interval
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	_, err = es.Indices.Create("entsoe")
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	var countSuccessful uint64
	total := len(lines)

	for _, line := range lines {
		id, err := hash(fmt.Sprintf("%s-%s-%s-%s", line.Region, line.Variable, line.Attribute, line.UTCTimestamp.Format("2006-01-02T15:04:05")))
		if err != nil {
			log.Fatalf("Cannot hash id %d: %s", line, err)
		}

		// Prepare the data payload: encode article to JSON
		//
		data, err := json.Marshal(map[string]any{
			"region":        line.Region,
			"variable":      line.Variable,
			"attribute":     line.Attribute,
			"utc_timestamp": line.UTCTimestamp.Format("2006-01-02T15:04:05"),
			"value":         line.Value,
		})
		if err != nil {
			log.Fatalf("Cannot encode article %d: %s", line, err)
		}

		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//
		// Add an item to the BulkIndexer
		//
		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				// Action field configures the operation to perform (index, create, delete, update)
				Action: "index",

				// DocumentID is the (optional) document ID
				DocumentID: id,

				// Body is an `io.Reader` with the payload
				Body: bytes.NewReader(data),

				// OnSuccess is called for each successful operation
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					count := atomic.AddUint64(&countSuccessful, 1)
					if count%100000 == 0 {
						fmt.Printf("Items: %d/%d (%d%%)\n", count, total, int(math.Round(float64(count)/float64(total)*100)))
					}
				},

				// OnFailure is called for each failed operation
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("ERROR: %s", err)
					} else {
						log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			log.Fatalf("Unexpected error: %s", err)
		}
	}

	if err := bi.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	logrus.Println("Write Finished. Time:", time.Since(start))
}
