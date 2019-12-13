package elasticsearch

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
)

func ConnByStr(httpAddr string) (*elastic.Client, error) {
	return conn(httpAddr)
}

func conn(httpAddr string) (*elastic.Client, error) {
	ctx := context.Background()

	client, err := elastic.NewClient()
	if err != nil {
		return nil, err
	}

	info, code, err := client.Ping(httpAddr).Do(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	esversion, err := client.ElasticsearchVersion(httpAddr)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Elasticsearch version %s\n", esversion)

	return client, nil
}
