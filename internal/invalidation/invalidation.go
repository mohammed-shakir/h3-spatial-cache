package invalidation

import "context"

type Interface interface {
	Start(ctx context.Context) error
}
