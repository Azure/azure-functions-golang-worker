package sdk

import (
	"strings"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// --- HTTP-specific options ---

// WithMethods sets the allowed HTTP methods for an HTTP trigger.
func WithMethods(methods ...string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.HTTPBinding != nil {
			b.HTTPBinding.Methods = methods
		}
	}
}

// WithAuth sets the authorization level for an HTTP trigger.
func WithAuth(level string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.HTTPBinding != nil {
			b.HTTPBinding.AuthLevel = level
		}
	}
}

// WithRoute sets the route for an HTTP trigger.
func WithRoute(route string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.HTTPBinding != nil {
			b.HTTPBinding.Route = route
		}
	}
}

// --- Timer-specific options ---

// WithSchedule sets the NCrontab CRON expression for a timer trigger.
func WithSchedule(schedule string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.TimerBinding != nil {
			b.TimerBinding.Schedule = schedule
		}
	}
}

// --- CosmosDB-specific options ---

// withCosmos applies f to the trigger's CosmosDBBinding when one is present.
// Options for other trigger types are silently no-ops on non-Cosmos triggers.
func withCosmos(f func(*bindings.CosmosDBBinding)) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.CosmosDBBinding != nil {
			f(b.CosmosDBBinding)
		}
	}
}

// WithDatabase sets the CosmosDB database name.
func WithDatabase(dbName string) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.DatabaseName = dbName })
}

// WithContainer sets the CosmosDB container name.
func WithContainer(containerName string) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.ContainerName = containerName })
}

// WithCreateLeaseContainerIfNotExists controls whether the CosmosDB change-feed
// extension automatically creates the lease container when it does not already
// exist. When false (default) the lease container must be provisioned out of
// band before the function starts.
func WithCreateLeaseContainerIfNotExists(enabled bool) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.CreateLeaseContainerIfNotExists = enabled })
}

// WithLeaseContainer overrides the name of the container the CosmosDB
// change-feed extension uses to store leases. Defaults to "leases".
func WithLeaseContainer(name string) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.LeaseContainerName = name })
}

// WithLeaseDatabase overrides the name of the database that contains the lease
// container. Defaults to the monitored database.
func WithLeaseDatabase(name string) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.LeaseDatabaseName = name })
}

// WithLeaseConnection sets the app-setting name that holds the connection
// string for the account hosting the lease container. Defaults to the
// monitored account's connection.
func WithLeaseConnection(connection string) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.LeaseConnection = connection })
}

// WithLeaseContainerPrefix sets a prefix used when multiple triggers share the
// same lease container. Required to avoid collisions when two functions
// monitor different containers but reuse the same leases container.
func WithLeaseContainerPrefix(prefix string) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.LeaseContainerPrefix = prefix })
}

// WithLeaseContainerThroughput sets the RU/s provisioned for the lease
// container when it is auto-created by [WithCreateLeaseContainerIfNotExists].
// Ignored if the container already exists.
//
// Note: the option is singular (LeaseContainer) but the underlying host
// binding key is plural (leasesContainerThroughput); this mirrors the
// property name the Functions host expects.
func WithLeaseContainerThroughput(ruPerSecond int) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.LeasesContainerThroughput = ruPerSecond })
}

// WithFeedPollDelay sets the delay between polls of a partition for new
// changes on the feed after all current changes are drained. Truncated to
// millisecond precision. Default is 5s.
//
// Positive durations below 1ms truncate to 0 and are omitted from the
// serialized binding, so the host default applies. Pass at least 1ms if you
// intend to override the default.
func WithFeedPollDelay(d time.Duration) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.FeedPollDelay = int(d.Milliseconds()) })
}

// WithLeaseRenewInterval sets the renew interval for leases the trigger
// currently holds. Truncated to millisecond precision. Default is 17s.
// Positive durations below 1ms truncate to 0 and fall back to the host
// default.
func WithLeaseRenewInterval(d time.Duration) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.LeaseRenewInterval = int(d.Milliseconds()) })
}

// WithLeaseAcquireInterval sets how often the trigger checks whether
// partitions are distributed evenly across known host instances. Truncated to
// millisecond precision. Default is 13s. Positive durations below 1ms
// truncate to 0 and fall back to the host default.
func WithLeaseAcquireInterval(d time.Duration) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.LeaseAcquireInterval = int(d.Milliseconds()) })
}

// WithLeaseExpirationInterval sets how long a lease is held on a partition
// before it expires and can be picked up by another instance. Truncated to
// millisecond precision. Default is 60s. Positive durations below 1ms
// truncate to 0 and fall back to the host default.
func WithLeaseExpirationInterval(d time.Duration) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.LeaseExpirationInterval = int(d.Milliseconds()) })
}

// WithMaxItemsPerInvocation caps the number of documents delivered to a
// single invocation of the trigger.
func WithMaxItemsPerInvocation(n int) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.MaxItemsPerInvocation = n })
}

// WithStartFromBeginning controls whether the trigger starts reading the
// change feed from the beginning of the container's history rather than
// from the time the function first runs. Mutually exclusive with
// [WithStartFromTime]; if both are supplied, the extension prefers
// StartFromTime.
func WithStartFromBeginning(enabled bool) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.StartFromBeginning = enabled })
}

// WithStartFromTime sets the point in time from which the change-feed read
// operation begins. The recommended format is ISO 8601 with the UTC
// designator, e.g. "2021-02-16T14:19:29Z".
func WithStartFromTime(iso8601 string) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) { c.StartFromTime = iso8601 })
}

// WithPreferredLocations sets preferred regions for a geo-replicated CosmosDB
// account. Values are joined with commas as required by the binding format,
// e.g. WithPreferredLocations("East US", "North Europe"). Empty strings are
// filtered out.
func WithPreferredLocations(locations ...string) Option {
	return withCosmos(func(c *bindings.CosmosDBBinding) {
		filtered := locations[:0:0]
		for _, l := range locations {
			if l != "" {
				filtered = append(filtered, l)
			}
		}
		c.PreferredLocations = strings.Join(filtered, ",")
	})
}

// --- EventHub-specific options ---

// WithEventHubName sets the EventHub name.
func WithEventHubName(name string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.EventHubBinding != nil {
			b.EventHubBinding.EventHubName = name
		}
	}
}

// WithConsumerGroup sets the EventHub consumer group.
func WithConsumerGroup(group string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.EventHubBinding != nil {
			b.EventHubBinding.ConsumerGroup = group
		}
	}
}

// --- Queue-specific options (Service Bus + Storage Queue) ---

// WithQueueName sets the queue name for a Service Bus queue trigger or
// a Storage Queue trigger.
func WithQueueName(queueName string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil {
			if b.ServiceBusBinding != nil {
				b.ServiceBusBinding.QueueName = queueName
			}
			if b.QueueBinding != nil {
				b.QueueBinding.QueueName = queueName
			}
		}
	}
}

// --- ServiceBus Topic-specific options ---

// WithTopicName sets the Service Bus topic name.
func WithTopicName(topicName string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.ServiceBusBinding != nil {
			b.ServiceBusBinding.TopicName = topicName
		}
	}
}

// WithSubscriptionName sets the Service Bus subscription name.
func WithSubscriptionName(subscriptionName string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.ServiceBusBinding != nil {
			b.ServiceBusBinding.SubscriptionName = subscriptionName
		}
	}
}

// --- Blob-specific options ---

// WithPath sets the blob container path for a Blob trigger.
func WithPath(path string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.BlobBinding != nil {
			b.BlobBinding.Path = path
		}
	}
}

// WithSource sets the event source for a Blob trigger.
// Use "EventGrid" for Flex Consumption (required) or "LogsAndContainerScan" for polling.
func WithSource(source string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.BlobBinding != nil {
			b.BlobBinding.Source = source
		}
	}
}

// --- SQL-specific options ---

// WithTable sets the SQL table name for a SQL trigger. The value should be
// the fully qualified table name including schema (e.g. "dbo.Products").
// Panics if table is empty — a SQL trigger cannot function without a target table.
func WithTable(table string) Option {
	if table == "" {
		panic("WithTable: table name must not be empty")
	}
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.SQLBinding != nil {
			b.SQLBinding.TableName = table
		}
	}
}

// WithLeasesTable overrides the name of the table the SQL extension uses to
// store change-tracking leases. If not set, the host generates a default
// name of the form Leases_{FunctionId}_{TableId}.
func WithLeasesTable(name string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.SQLBinding != nil {
			b.SQLBinding.LeasesTableName = name
		}
	}
}

// --- Shared options (work across multiple trigger types) ---

// WithConnection sets the connection string setting name (i.e. the name of
// the app setting that holds the actual connection string).
// Works with CosmosDB, EventHub, ServiceBus, Blob, and SQL triggers.
// For SQL it populates the binding's "connectionStringSetting" field; for
// the others it populates "connection".
func WithConnection(connection string) Option {
	return func(rf *RegisteredFunction) {
		b := rf.triggerBinding()
		if b == nil {
			return
		}
		if b.CosmosDBBinding != nil {
			b.CosmosDBBinding.Connection = connection
		}
		if b.EventHubBinding != nil {
			b.EventHubBinding.Connection = connection
		}
		if b.ServiceBusBinding != nil {
			b.ServiceBusBinding.Connection = connection
		}
		if b.BlobBinding != nil {
			b.BlobBinding.Connection = connection
		}
		if b.QueueBinding != nil {
			b.QueueBinding.Connection = connection
		}
		if b.SQLBinding != nil {
			b.SQLBinding.ConnectionStringSetting = connection
		}
	}
}

// WithCardinality sets the trigger cardinality ("one" or "many").
// Works with EventHub and ServiceBus triggers.
func WithCardinality(cardinality string) Option {
	return func(rf *RegisteredFunction) {
		b := rf.triggerBinding()
		if b == nil {
			return
		}
		if b.EventHubBinding != nil {
			b.EventHubBinding.Cardinality = cardinality
		}
		if b.ServiceBusBinding != nil {
			b.ServiceBusBinding.Cardinality = cardinality
		}
	}
}

// WithIsSessionsEnabled sets whether sessions are enabled.
// Works with ServiceBus triggers.
func WithIsSessionsEnabled(enabled bool) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.ServiceBusBinding != nil {
			b.ServiceBusBinding.IsSessionsEnabled = enabled
		}
	}
}

// WithRetry adds a retry policy to a function.
func WithRetry(retry *RetryOptions) Option {
	return func(rf *RegisteredFunction) {
		rf.Retry = retry
	}
}

// --- Helpers ---

// TriggerBinding returns a pointer to the first binding (the trigger), or nil.
// This is exported for use by external trigger modules.
func (rf *RegisteredFunction) TriggerBinding() *bindings.Binding {
	if len(rf.RawBindings) > 0 {
		return &rf.RawBindings[0]
	}
	return nil
}

// triggerBinding is an unexported alias for internal use.
func (rf *RegisteredFunction) triggerBinding() *bindings.Binding {
	return rf.TriggerBinding()
}
