package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"time"
)

var dbPool *pgxpool.Pool

var createStoreQuery = `CREATE TABLE IF NOT EXISTS store (
key TEXT PRIMARY KEY,
value TEXT NOT NULL
);`

var createAppliedRequestsQuery = `CREATE TABLE IF NOT EXISTS applied_requests (
req_id TEXT PRIMARY KEY,
found BOOLEAN NOT NULL,
value TEXT NOT NULL DEFAULT ''
);`

var createRaftMetadataQuery = `CREATE TABLE IF NOT EXISTS raft_metadata (
id BOOLEAN PRIMARY KEY DEFAULT TRUE,
last_applied BIGINT NOT NULL
)`

var getAppliedRequestQuery = `SELECT found, value FROM applied_requests WHERE req_id = $1;`
var updateAppliedRequestQuery = `INSERT INTO applied_requests (req_id, found, value)
VALUES ($1, $2, $3);`

var getAllAppliedRequestsQuery = `SELECT req_id, found, value FROM applied_requests;`
var getValueQuery = `SELECT value from store WHERE key = $1;`

var updateValueQuery = `INSERT INTO store (key, value) 
VALUES ($1, $2) 
ON CONFLICT (key) DO UPDATE 
SET value = EXCLUDED.value;`

var deleteValueQuery = `DELETE FROM store WHERE key = $1;`
var getAllStoreQuery = `SELECT key, value FROM store;`

var getRaftMetadataQuery = `SELECT last_applied from raft_metadata WHERE id = TRUE;`

var updateRaftMetadataQuery = `INSERT INTO raft_metadata (id, last_applied) 
VALUES (TRUE, $1)
ON CONFLICT (id) DO UPDATE
SET last_applied = GREATEST(raft_metadata.last_applied, EXCLUDED.last_applied);`

func initDB() error {
	ctx, cancelCtx := context.WithTimeout(context.Background(), time.Duration(time.Second*5))
	defer cancelCtx() //this will get executed as soon as the timeout expires

	connString := os.Getenv("DATABASE_URL")

	if connString == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}

	pool, err := pgxpool.New(ctx, connString)

	if err != nil {
		return fmt.Errorf("Unable to create connection pool: %v\n", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("Unable to ping db: %w", err)
	}

	dbPool = pool

	if err := initSchema(ctx); err != nil {
		return fmt.Errorf("Error initialising schema: %w", err)
	}

	return nil
}

func initSchema(ctx context.Context) error {
	if dbPool == nil {
		return fmt.Errorf("db pool is null")
	}

	tx, err := dbPool.Begin(ctx)

	if err != nil {
		return fmt.Errorf("Err starting transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, createStoreQuery); err != nil {
		return fmt.Errorf("Unable to create Store Table: %w", err)
	}

	if _, err = tx.Exec(ctx, createAppliedRequestsQuery); err != nil {
		return fmt.Errorf("Unable to create Applied Requests Table: %w", err)
	}

	if _, err := tx.Exec(ctx, createRaftMetadataQuery); err != nil {
		return fmt.Errorf("Unable to create Raft Metadata Table: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("Commit transaction failed: %w", err)
	}

	return nil
}

// get value from an entry (~store[key])
func getValue(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := dbPool.QueryRow(ctx, getValueQuery, key).Scan(&value)

	if err != nil {
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}

	return value, true, nil
}

func updateValue(ctx context.Context, key string, value string, reqId string, lastAppliedIndex int) (AppliedResult, error) {
	tx, err := dbPool.Begin(ctx)

	if err != nil {
		return AppliedResult{Found: false}, err
	}

	defer tx.Rollback(ctx)

	var appliedResult AppliedResult
	err = tx.QueryRow(ctx, getAppliedRequestQuery, reqId).Scan(&appliedResult.Found, &appliedResult.Value)

	if err == nil {
		if _, err := tx.Exec(ctx, updateRaftMetadataQuery, lastAppliedIndex); err != nil {
			return AppliedResult{Found: false}, err
		}

		if err := tx.Commit(ctx); err != nil {
			return AppliedResult{Found: false}, err
		}

		return appliedResult, nil
	}
	if err != pgx.ErrNoRows {
		return AppliedResult{Found: false}, err
	}

	//updating the value and the applied request
	if _, err := tx.Exec(ctx, updateValueQuery, key, value); err != nil {
		return AppliedResult{Found: false}, err
	}

	if _, err := tx.Exec(ctx, updateAppliedRequestQuery, reqId, true, value); err != nil {
		return AppliedResult{Found: false}, err
	}

	if _, err := tx.Exec(ctx, updateRaftMetadataQuery, lastAppliedIndex); err != nil {
		return AppliedResult{Found: false}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return AppliedResult{Found: false}, err
	}

	return AppliedResult{Found: true, Value: value}, nil
}

func deleteValue(ctx context.Context, key string, reqId string, lastAppliedIndex int) (AppliedResult, error) {
	tx, err := dbPool.Begin(ctx)

	if err != nil {
		return AppliedResult{Found: false}, err
	}

	defer tx.Rollback(ctx)

	var appliedResult AppliedResult
	err = tx.QueryRow(ctx, getAppliedRequestQuery, reqId).Scan(&appliedResult.Found, &appliedResult.Value)

	if err == nil {
		if _, err := tx.Exec(ctx, updateRaftMetadataQuery, lastAppliedIndex); err != nil {
			return AppliedResult{Found: false}, err
		}

		if err := tx.Commit(ctx); err != nil {
			return AppliedResult{Found: false}, err
		}
		return appliedResult, err
	}
	if err != pgx.ErrNoRows {
		return AppliedResult{Found: false}, err
	}

	//delete the entry from store and update applied request
	deleteTag, err := tx.Exec(ctx, deleteValueQuery, key)

	if err != nil {
		return AppliedResult{Found: false}, err
	}

	deleted := deleteTag.RowsAffected() > 0

	if _, err = tx.Exec(ctx, updateAppliedRequestQuery, reqId, deleted, ""); err != nil {
		return AppliedResult{Found: false}, err
	}

	
	if _, err := tx.Exec(ctx, updateRaftMetadataQuery, lastAppliedIndex); err != nil {
		return AppliedResult{Found: false}, err
	}
	
	if err := tx.Commit(ctx); err != nil {
		return AppliedResult{Found: false}, err
	}

	return AppliedResult{Found: deleted}, nil
}

func updateFromSnapshot(ctx context.Context, snapshotFile *SnapshotFile) error {
	tx, err := dbPool.Begin(ctx)

	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	//delete then update
	if _, err := tx.Exec(ctx, `TRUNCATE TABLE store, applied_requests`); err != nil {
		return err
	}

	for key, value := range snapshotFile.Data {
		_, err := tx.Exec(ctx, updateValueQuery, key, value)
		if err != nil {
			return err
		}
	}

	for reqId, appliedResult := range snapshotFile.AppliedReqIDs {
		_, err := tx.Exec(ctx, updateAppliedRequestQuery, reqId, appliedResult.Found, appliedResult.Value)
		if err != nil {
			return err
		}
	}

	//also update the raft metadata
	if _, err := tx.Exec(ctx, updateRaftMetadataQuery, snapshotFile.LastIncludedIndex); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func updateSnapshotFromDB(ctx context.Context, snapshotFile *SnapshotFile) error {
	tx, err := dbPool.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, getAllStoreQuery)
	if err != nil {
		return err
	}

	defer rows.Close()

	snapshotData := make(map[string]string)
	for rows.Next() {
		var key string
		var value string

		if err := rows.Scan(&key, &value); err != nil {
			return err
		}
		snapshotData[key] = value
	}

	if err := rows.Err(); err != nil {
		return err
	}

	snapshotFile.Data = snapshotData

	rows, err = tx.Query(ctx, getAllAppliedRequestsQuery)

	if err != nil {
		return err
	}

	appliedRequests := make(map[string]AppliedResult)
	for rows.Next() {
		var reqId string
		var found bool
		var value string

		if err := rows.Scan(&reqId, &found, &value); err != nil {
			return err
		}
		appliedRequests[reqId] = AppliedResult{
			Found: found,
			Value: value,
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	snapshotFile.AppliedReqIDs = appliedRequests

	return tx.Commit(ctx)
}

func appliedRequestExist(ctx context.Context, reqId *string) (AppliedResult, bool, error) {
	var appliedResult AppliedResult

	err := dbPool.QueryRow(ctx, getAppliedRequestQuery, reqId).Scan(&appliedResult.Found, &appliedResult.Value)

	if err == pgx.ErrNoRows {
		return AppliedResult{}, false, nil
	}

	if err != nil {
		return AppliedResult{}, false, err
	}

	return appliedResult, true, nil
}

//get current raft metadata
func getRaftMetadata(ctx context.Context) (int, bool, error){
	var lastApplied int

	err := dbPool.QueryRow(ctx, getRaftMetadataQuery).Scan(&lastApplied)

	if err == pgx.ErrNoRows {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	return lastApplied, true, nil
}