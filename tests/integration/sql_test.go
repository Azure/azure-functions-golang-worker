package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
)

// sqlTestPassword returns the SA password for the test SQL Server
// emulator. Override via SQL_TEST_PASSWORD; the default matches the one
// docker-compose uses when the env var is unset.
func sqlTestPassword() string {
	if p := os.Getenv("SQL_TEST_PASSWORD"); p != "" {
		return p
	}
	return "StrongP@ssw0rd!"
}

func sqlMasterConnStr() string {
	return fmt.Sprintf(
		"server=127.0.0.1;port=1433;database=master;user id=sa;password=%s;encrypt=disable;TrustServerCertificate=true",
		sqlTestPassword(),
	)
}

func sqlTestConnStr() string {
	return fmt.Sprintf(
		"server=127.0.0.1;port=1433;database=test;user id=sa;password=%s;encrypt=disable;TrustServerCertificate=true",
		sqlTestPassword(),
	)
}

func sqlEnv() map[string]string {
	return map[string]string{
		"AzureWebJobsStorage":      "UseDevelopmentStorage=true",
		"FUNCTIONS_WORKER_RUNTIME": "native",
		"AzureWebJobsSqlConnectionString": fmt.Sprintf(
			"Server=127.0.0.1,1433;Database=test;User Id=sa;Password=%s;TrustServerCertificate=True;",
			sqlTestPassword(),
		),
	}
}

// ensureSQLChangeTracking provisions the test database and table the
// sqlTrigger sample monitors. Idempotent: re-enabling change tracking is
// tolerated via isChangeTrackingAlreadyEnabled.
func ensureSQLChangeTracking(t *testing.T) {
	t.Helper()

	masterDB, err := sql.Open("sqlserver", sqlMasterConnStr())
	if err != nil {
		t.Fatalf("failed to open master sql connection: %v", err)
	}
	defer masterDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := masterDB.PingContext(ctx); err != nil {
		t.Fatalf("failed to ping master sql: %v", err)
	}

	masterSetup := []string{
		`IF DB_ID('test') IS NULL CREATE DATABASE test`,
	}
	for _, q := range masterSetup {
		if _, err := masterDB.ExecContext(ctx, q); err != nil {
			t.Fatalf("master setup query failed: %v\nquery: %s", err, q)
		}
	}

	// CHANGE_TRACKING must run as its own batch against master.
	if _, err := masterDB.ExecContext(ctx,
		`ALTER DATABASE test SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)`,
	); err != nil && !isChangeTrackingAlreadyEnabled(err) {
		t.Fatalf("failed to enable change tracking on database: %v", err)
	}

	testDB, err := sql.Open("sqlserver", sqlTestConnStr())
	if err != nil {
		t.Fatalf("failed to open test sql connection: %v", err)
	}
	defer testDB.Close()

	// Create table if it doesn't exist; truncate if it does. Avoid DROP TABLE
	// because that invalidates the OBJECT_ID stored in az_func.GlobalState,
	// causing subsequent runs (without a host restart) to hang waiting for the
	// SQL extension to re-register a listener for the new OBJECT_ID.
	tableSetup := []string{
		`IF OBJECT_ID('dbo.Products', 'U') IS NULL
			CREATE TABLE dbo.Products (
				ProductId INT PRIMARY KEY,
				Name      NVARCHAR(100) NOT NULL,
				Cost      INT NOT NULL
			)
		ELSE
			TRUNCATE TABLE dbo.Products`,
	}
	for _, q := range tableSetup {
		if _, err := testDB.ExecContext(ctx, q); err != nil {
			t.Fatalf("table setup query failed: %v\nquery: %s", err, q)
		}
	}

	if _, err := testDB.ExecContext(ctx,
		`ALTER TABLE dbo.Products ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)`,
	); err != nil && !isChangeTrackingAlreadyEnabled(err) {
		t.Fatalf("failed to enable change tracking on table: %v", err)
	}

	// Clean stale extension state so the listener re-registers cleanly.
	if _, err := testDB.ExecContext(ctx,
		`IF SCHEMA_ID('az_func') IS NOT NULL AND OBJECT_ID('az_func.GlobalState', 'U') IS NOT NULL
			DELETE FROM az_func.GlobalState WHERE UserTableID = OBJECT_ID('dbo.Products')`,
	); err != nil {
		// Non-fatal: the schema may not exist on a fresh container.
		t.Logf("warning: could not clean az_func.GlobalState: %v", err)
	}
}

// isChangeTrackingAlreadyEnabled reports whether err is the benign
// "change tracking already enabled" response. The real error number lives
// in mssql.Error.All (5088 for database-level, 4996 for table-level);
// err.Error() only returns the generic trailing "ALTER ... failed" message.
func isChangeTrackingAlreadyEnabled(err error) bool {
	if err == nil {
		return false
	}
	var sqlErr mssql.Error
	if !errors.As(err, &sqlErr) {
		return false
	}
	for _, e := range sqlErr.All {
		if e.Number == 5088 || e.Number == 4996 {
			return true
		}
	}
	return sqlErr.Number == 5088 || sqlErr.Number == 4996
}

// waitForSQLTriggerReady polls az_func.GlobalState for the per-(function,
// table) state row the SQL extension writes once SqlTriggerListener.StartAsync
// completes. Its appearance is deterministic proof the listener is live.
func waitForSQLTriggerReady(t *testing.T, db *sql.DB, schema, table string, timeout time.Duration) {
	t.Helper()

	fullName := schema + "." + table
	deadline := time.Now().Add(timeout)
	const query = `SELECT COUNT(*) FROM az_func.GlobalState WHERE UserTableID = OBJECT_ID(@p1)`

	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var n int
		err := db.QueryRowContext(ctx, query, fullName).Scan(&n)
		cancel()
		if err == nil && n > 0 {
			return
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("SQL trigger listener did not register for %s within %s (last poll error: %v)", fullName, timeout, lastErr)
	}
	t.Fatalf("SQL trigger listener did not register for %s within %s", fullName, timeout)
}

// TestSQLTriggerFiresOnChanges starts the sqlTrigger sample once and walks
// Insert → Update → Delete sequentially against the monitored table,
// verifying the trigger fires and the handler observes each operation.
func TestSQLTriggerFiresOnChanges(t *testing.T) {
	requireAzurite(t)
	requireSQLServer(t)
	ensureSQLChangeTracking(t)

	proc := StartFuncHost(t, "sqlTrigger", 7209, sqlEnv(), 40*time.Second)

	testDB, err := sql.Open("sqlserver", sqlTestConnStr())
	if err != nil {
		t.Fatalf("failed to open test sql connection: %v", err)
	}
	defer testDB.Close()

	// Readiness gate: poll the extension's per-(function, table) state
	// row directly; the v3.1.527 extension does not emit a stable
	// Information-level "started" log line.
	waitForSQLTriggerReady(t, testDB, "dbo", "Products", 60*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Insert
	if _, err := testDB.ExecContext(ctx,
		`INSERT INTO dbo.Products (ProductId, Name, Cost) VALUES (@p1, @p2, @p3)`,
		1, "Widget", 100,
	); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	proc.AssertLogContains("Executing 'Functions.productsChanged'", 45*time.Second)
	proc.AssertLogContains("operation=Insert", 5*time.Second)
	proc.AssertLogContains("product_id=1", 5*time.Second)
	proc.AssertLogContains("Executed 'Functions.productsChanged' (Succeeded", 15*time.Second)

	// Update
	if _, err := testDB.ExecContext(ctx,
		`UPDATE dbo.Products SET Cost = @p1 WHERE ProductId = @p2`,
		150, 1,
	); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	proc.AssertLogContains("operation=Update", 45*time.Second)

	// Delete
	if _, err := testDB.ExecContext(ctx,
		`DELETE FROM dbo.Products WHERE ProductId = @p1`,
		1,
	); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	proc.AssertLogContains("operation=Delete", 45*time.Second)

	// Sanity: nothing exploded mid-run.
	//
	// Allowlist rationale (one entry per known noise source — keep these
	// narrow so an unrelated SQL-extension regression still surfaces):
	//   - "SqlTriggerListener" / "SqlTableChangeMonitor": SQL-extension
	//     component categories that emit informational lines containing the
	//     literal word "error" during normal change-tracking/lease polling
	//     (e.g. "Encountered exception ... transient SQL error ... retrying").
	//   - "ConsecutiveErrors=": host metric dump key.
	//   - `"UseStdErrorStreamForErrorsOnly"`: host-config dump key.
	proc.AssertLogNotContainsError(
		"SqlTriggerListener",
		"SqlTableChangeMonitor",
		"ConsecutiveErrors=",
		`"UseStdErrorStreamForErrorsOnly"`,
	)
}
