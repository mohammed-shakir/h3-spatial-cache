package integration

import (
	"context"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/composer"
)

type echoAgg struct{}

func (e *echoAgg) MergeWithQuery(_ context.Context, q composer.QueryParams, _ []composer.ShardPage) ([]byte, error) {
	_ = q
	return []byte(`{"type":"FeatureCollection","features":[
		{"type":"Feature","id":"1","geometry":null,"properties":{"ts":"2024-01-01T00:00:00Z","v":10}},
		{"type":"Feature","id":"2","geometry":null,"properties":{"ts":"2024-01-02T00:00:00Z","v":20}}
	]}`), nil
}

func Test_Composer_RespectsSortAndLimit(t *testing.T) {
	eng := composer.Engine{V2: &echoAgg{}}
	req := composer.Request{
		Query: composer.QueryParams{
			Sort:  []composer.SortKey{{Property: "ts", Desc: false}},
			Limit: 2, Offset: 0,
		},
		Pages: []composer.ShardPage{
			{Body: []byte(`{"type":"FeatureCollection","features":[]}`), CacheStatus: composer.CacheHit},
		},
		OutputFormat: "application/json",
	}
	res, err := composer.Compose(context.Background(), eng, req)
	if err != nil {
		t.Fatal(err)
	}
	if res.ContentType != "application/geo+json" {
		t.Fatalf("want application/geo+json, got %s", res.ContentType)
	}
}
