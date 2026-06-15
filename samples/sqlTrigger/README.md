# SQL Trigger Sample

An Azure Function that triggers when rows change in a SQL Server / Azure SQL
table tracked by [SQL Change Tracking](https://learn.microsoft.com/sql/relational-databases/track-changes/about-change-tracking-sql-server).

## What this sample demonstrates

- A typed SQL-trigger handler `func(ctx context.Context, changes []bindings.SQLChange) error` — row changes are deserialized directly from the gRPC `InvocationRequest` into the `SQLChange` slice, no SDK client needed (the **core trigger** model in [`TECHNICAL_SPEC.md`](../../TECHNICAL_SPEC.md) section 4).
- Per-row structured logging with `slog.InfoContext` carrying the change `operation` (Insert / Update / Delete) plus user-supplied row attributes.
- Caller-controlled row decoding: `SQLChange.Item` is `json.RawMessage`, so each handler unmarshals into its own row struct (here: `Product`).

## Prerequisites

- [Go 1.25+](https://go.dev/dl/)
- [Azure Functions Core Tools](https://www.npmjs.com/package/azure-functions-core-tools/v/4.12.0) 4.12.0 or later (includes Go worker support):
  ```bash
  npm i -g azure-functions-core-tools@4 --unsafe-perm true
  ```
- A SQL Server / Azure SQL database with Change Tracking enabled (see below).

## Setup

```bash
cd samples/sqlTrigger
go mod init myapp
go get github.com/azure/azure-functions-golang-worker
go mod tidy
```

### Enable Change Tracking

The SQL trigger relies on Change Tracking, which must be enabled at both
the database and the table level *before* you start the function. Follow
the official guide to enable it on your database and on the table you want
to monitor (the sample expects a table named `dbo.Products`):

- [Set up change tracking (Azure Functions SQL trigger prerequisites)](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql-trigger?tabs=isolated-process,extensionv4&pivots=programming-language-csharp#set-up-change-tracking-required)

Update `local.settings.json` with your SQL connection string if you're not
using the default local SQL Server credentials.

### Sample schema

The trigger handler in `main.go` expects this table (column types match
the `Product` struct):

```sql
CREATE TABLE dbo.Products (
    ProductId INT NOT NULL PRIMARY KEY,
    Name      NVARCHAR(100) NOT NULL,
    Cost      INT NOT NULL
);
```

Make sure to also `ENABLE CHANGE_TRACKING` on this table (covered by the
link above).

The optional extended example under [Reading and writing SQL data from
your handler](#reading-and-writing-sql-data-from-your-handler) additionally
expects two companion tables:

```sql
CREATE TABLE dbo.Orders (
    OrderId   INT NOT NULL PRIMARY KEY,
    ProductId INT NOT NULL
);

CREATE TABLE dbo.ProductSummaries (
    ProductId  INT NOT NULL PRIMARY KEY,
    Name       NVARCHAR(100) NOT NULL,
    Cost       INT NOT NULL,
    OrderCount INT NOT NULL,
    UpdatedAt  DATETIME2 NOT NULL
);
```

These two are **not** tracked by Change Tracking — only `dbo.Products` is.

## Run

```bash
func start
```

`func start` automatically builds the Go project before launching. To skip
the build step (e.g., if you've already built manually), use:

```bash
func start --no-build
```

## Test

```sql
INSERT INTO dbo.Products VALUES (1, 'Widget', 100);
UPDATE dbo.Products SET Cost = 150 WHERE ProductId = 1;
DELETE FROM dbo.Products WHERE ProductId = 1;
```

The function will fire on each committed change (with a few seconds of
polling latency) and log the operation + row data.

## Reading and writing SQL data from your handler

Use [`database/sql`](https://pkg.go.dev/database/sql) with the
[`github.com/microsoft/go-mssqldb`](https://pkg.go.dev/github.com/microsoft/go-mssqldb)
driver to read related rows or write summary rows from inside the trigger
handler. The pattern is: open one `*sql.DB` pool at startup, share it
across invocations, parameterize every query.

The example below is a self-contained replacement for `main.go` that extends
the shipped sample with a SELECT (looking up a related row before acting)
and a `MERGE` upsert (writing a denormalized summary row keyed by primary
key). You can paste it in as-is after creating the companion `dbo.Orders`
and `dbo.ProductSummaries` tables.

```go
package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log/slog"
    "os"

    _ "github.com/microsoft/go-mssqldb"

    "github.com/azure/azure-functions-golang-worker/sdk"
    "github.com/azure/azure-functions-golang-worker/sdk/bindings"
    "github.com/azure/azure-functions-golang-worker/worker"
)

type Product struct {
    ProductID int    `json:"ProductId"`
    Name      string `json:"Name"`
    Cost      int    `json:"Cost"`
}

// db is opened once at startup and reused across invocations. *sql.DB is
// itself a connection pool — do not open one per invocation.
var db *sql.DB

func main() {
    var err error
    // For Managed Identity in production, use a connection string with
    // "Authentication=ActiveDirectoryDefault" — go-mssqldb integrates with
    // azidentity's DefaultAzureCredential under that auth mode.
    db, err = sql.Open("sqlserver", os.Getenv("AzureWebJobsSqlConnectionString"))
    if err != nil {
        slog.Error("failed to open sql pool", "error", err)
        os.Exit(1)
    }
    db.SetMaxOpenConns(10)

    app := sdk.FunctionApp()
    app.SQL("productsChanged", productsChanged,
        sdk.WithTable("dbo.Products"),
        sdk.WithConnection("AzureWebJobsSqlConnectionString"),
    )
    worker.Start(app)
}

// productsChanged is invoked with a batch of row changes captured from
// dbo.Products via SQL Change Tracking. For each change it maintains a
// denormalized summary row in dbo.ProductSummaries.
func productsChanged(ctx context.Context, changes []bindings.SQLChange) error {
    for _, change := range changes {
        // 1. Decode the row payload into our struct.
        var p Product
        if err := json.Unmarshal(change.Item, &p); err != nil {
            return fmt.Errorf("decode row: %w", err)
        }

        // 2. On Delete, remove the corresponding summary row and move on.
        if change.Operation == bindings.SQLOperationDelete {
            if err := deleteSummary(ctx, p.ProductID); err != nil {
                return err
            }
            continue
        }

        // 3. On Insert/Update, enrich with a related read.
        orderCount, err := orderCountForProduct(ctx, p.ProductID)
        if err != nil {
            return err
        }

        // 4. Log what we're about to write (structured, context-aware).
        slog.InfoContext(ctx, "writing summary",
            "operation", change.Operation.String(),
            "product_id", p.ProductID,
            "order_count", orderCount,
        )

        // 5. Upsert the summary row keyed by primary key.
        if err := upsertSummary(ctx, p, orderCount); err != nil {
            return err
        }
    }
    return nil
}

// orderCountForProduct returns the number of orders referencing the given
// product.
func orderCountForProduct(ctx context.Context, productID int) (int, error) {
    const q = `SELECT COUNT(*) FROM dbo.Orders WHERE ProductId = @p1`
    var n int
    if err := db.QueryRowContext(ctx, q, productID).Scan(&n); err != nil {
        return 0, fmt.Errorf("read order count: %w", err)
    }
    return n, nil
}

// upsertSummary inserts or updates a denormalized summary row keyed by
// primary key.
func upsertSummary(ctx context.Context, p Product, orderCount int) error {
    const q = `
        MERGE dbo.ProductSummaries AS target
        USING (VALUES (@p1, @p2, @p3, @p4)) AS src(ProductId, Name, Cost, OrderCount)
            ON target.ProductId = src.ProductId
        WHEN MATCHED THEN
            UPDATE SET Name = src.Name, Cost = src.Cost,
                       OrderCount = src.OrderCount, UpdatedAt = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (ProductId, Name, Cost, OrderCount, UpdatedAt)
            VALUES (src.ProductId, src.Name, src.Cost, src.OrderCount, SYSUTCDATETIME());`
    if _, err := db.ExecContext(ctx, q, p.ProductID, p.Name, p.Cost, orderCount); err != nil {
        return fmt.Errorf("upsert summary: %w", err)
    }
    return nil
}

// deleteSummary removes a summary row when its source product is deleted.
func deleteSummary(ctx context.Context, productID int) error {
    const q = `DELETE FROM dbo.ProductSummaries WHERE ProductId = @p1`
    if _, err := db.ExecContext(ctx, q, productID); err != nil {
        return fmt.Errorf("delete summary: %w", err)
    }
    return nil
}
```

A few notes on the patterns above:

- **Parameter binding.** `go-mssqldb` accepts `@p1`-style positional names
  or [`sql.Named("name", value)`](https://pkg.go.dev/database/sql#Named)
  for explicit binding. Always use one or the other — never string-concat
  user input into SQL.
- **Upserts.** `MERGE` is the SQL-Server equivalent of an upsert and gives
  you key-based deduplication in a single round-trip. For high-throughput batch
  loads, prefer `mssql.CopyIn` — see the
  [`go-mssqldb` bulk-insert guide](https://github.com/microsoft/go-mssqldb#bulk-inserts).
- **Errors.** Returning a non-nil `error` from the handler signals the
  host to apply its retry policy. The SQL change itself stays committed —
  the trigger fires post-commit, so the only thing being retried is your
  summary-row logic.

## What this trigger is NOT

The SQL trigger is a polled change-tracking listener, NOT a synchronous
DB-side hook. Concretely:

- **Azure SQL / SQL Server only.** There is no equivalent for Azure
  Database for PostgreSQL, MySQL, or MariaDB.
- **Polled change tracking, not DB triggers.** The host extension polls
  `CHANGETABLE(CHANGES <table>, <version>)` every few seconds. There is no
  way to attach to `AFTER INSERT` / `BEFORE UPDATE` events.
- **Post-commit, not in-transaction.** Errors in the Go handler do not
  roll back the SQL change; they trigger the host's retry policy.
- **One function per table — no wildcards or arrays.**
