// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package source

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
	"google.golang.org/api/iterator"
)

type BigQuerySource struct {
	client           *bigquery.Client
	projectID        string
	datasetID        string
	tableID          string
	manifestInserter *bigquery.Inserter
	concurrentWrites int

	log *log.Entry
}

type ManifestRecord struct {
	ManifestID string
}

func (mr *ManifestRecord) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"manifestID": mr.ManifestID,
	}, "", nil
}

func NewBigQuerySource(concurrentWrites int, projectID string, datasetID string, tableID string, manifestDatasetID string, manifestTableID string) (*BigQuerySource, error) { // TODO: instrument config
	//projectID := "engineering-sandbox"
	//datasetID := "scratch"
	//tableID := "hackathon_test"
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %v", err)
	}

	inserter := client.Dataset(manifestDatasetID).Table(manifestTableID).Inserter()

	return &BigQuerySource{
		client:    client,
		projectID: projectID,
		datasetID: datasetID,
		tableID:   tableID,
		// manifestDatasetID: "scratch",
		// manifestTableID:   "hackathon_test_revetl_manifest",
		manifestInserter: inserter,
		concurrentWrites: concurrentWrites,

		log: log.WithFields(log.Fields{"source": "bigquery", "cloud": "GCP", "project": projectID, "dataset": datasetID}),
	}, nil
}

// Read will pull messages from BigQuery and spawn a goroutine per event for the targetWrite function
func (bq *BigQuerySource) Read(sf *sourceiface.SourceFunctions) error {
	bq.log.Infof("Reading messages from bigquery ...")

	defer bq.client.Close() // TODO: remind self of closing pattern and instrument accordingly.

	ctx := context.Background()
	table := bq.client.Dataset(bq.datasetID).Table(bq.tableID)
	it := table.Read(ctx)

	throttle := make(chan struct{}, bq.concurrentWrites)
	wg := sync.WaitGroup{}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		timePulled := time.Now().UTC()
		// TODO: Figure out sensible handling of timeCreated and related stats

		data := row[0].(string)
		manifestID := row[1].(string)

		ackFunc := func() {
			if err := bq.manifestInserter.Put(context.Background(), ManifestRecord{ManifestID: manifestID}); err != nil {
				fmt.Println(err)
			}
		}

		messages := []*models.Message{
			{
				Data:         []byte(data),
				PartitionKey: manifestID, // TODO: make  using this feature configurable
				AckFunc:      ackFunc,    // TODO: add acking
				TimeCreated:  timePulled, // TODO: fix this
				TimePulled:   timePulled,
			},
		} // Perhaps a partition key may be speicified from the data itself, perhaps that's not useful.
		throttle <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := sf.WriteToTarget(messages)

			// The Kinsumer client blocks unless we can checkpoint which only happens
			// on a successful write to the target.  As such we need to force an app
			// close in this scenario to allow it to reboot and hopefully continue.
			if err != nil {
				bq.log.WithFields(log.Fields{"error": err}).Fatal(err)
			}
			<-throttle
		}()

	}
	return nil
}

// Stop will halt the reader processing more events
func (bq *BigQuerySource) Stop() {
	bq.log.Warn("Cancelling Bigquery receive ...")
	bq.client.Close()
}

// GetID returns the identifier for this source
func (bq *BigQuerySource) GetID() string {
	return fmt.Sprintf("hackathon")
}
