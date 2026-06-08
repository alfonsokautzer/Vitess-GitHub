package vtgate

import (
	"context"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// ExecuteScatterQuery ensures that all shards are accounted for and errors are propagated.
func (sc *ScatterConn) ExecuteScatterQuery(ctx context.Context, keyspace string, shards []string, query string) (*Result, error) {
	// 1. Validate shard coverage
	if len(shards) == 0 {
		return nil, vterrors.Errorf(vterrors.Code_FAILED_PRECONDITION, "no shards provided for scatter query")
	}

	// 2. Execute and aggregate results
	results, err := sc.execute(ctx, keyspace, shards, query)
	if err != nil {
		// Ensure we don't return partial results on error
		return nil, vterrors.Wrap(err, "scatter query failed")
	}

	// 3. Strict validation: ensure all shards returned data or success
	if len(results) < len(shards) {
		return nil, vterrors.Errorf(vterrors.Code_UNAVAILABLE, "partial results detected: expected %d shards, got %d", len(shards), len(results))
	}

	return mergeResults(results), nil
}