package integration

import (
	"context"
	"testing"

	"github.com/mohammed-shakir/h3-spatial-cache/internal/composer"
)

type fakeAgg struct{}

func (f *fakeAgg) MergeWithQuery(_ context.Context, q composer.QueryParams, pages []composer.ShardPage) ([]byte, error) {
	_ = q
	_ = pages
	return []byte(`{"type":"FeatureCollection","features":[{"type":"Feature","id":"A","geometry":null,"properties":{"n":1}}]}`), nil
}

func Test_Composer_GeoJSON_CachedVsUncached_Identical(t *testing.T) {
	eng := composer.Engine{V2: &fakeAgg{}}

	reqCached := composer.Request{
		Query:        composer.QueryParams{Limit: 10, Offset: 0},
		Pages:        []composer.ShardPage{{Body: []byte(`{"type":"FeatureCollection","features":[]}`), CacheStatus: composer.CacheHit}},
		AcceptHeader: "application/geo+json",
	}
	reqUncached := reqCached
	reqUncached.Pages[0].CacheStatus = composer.CacheMiss

	gotCached, err := composer.Compose(context.Background(), eng, reqCached)
	if err != nil {
		t.Fatal(err)
	}
	gotUncached, err := composer.Compose(context.Background(), eng, reqUncached)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotCached.Body) != string(gotUncached.Body) {
		t.Fatalf("responses differ:\nCACHED  : %s\nUNCACHED: %s", string(gotCached.Body), string(gotUncached.Body))
	}
	if gotCached.ContentType != "application/geo+json" {
		t.Fatalf("content-type: %s", gotCached.ContentType)
	}
}
