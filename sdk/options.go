package sdk

import "github.com/azure/azure-functions-golang-worker/sdk/bindings"

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

// WithDatabase sets the CosmosDB database name.
func WithDatabase(dbName string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.CosmosDBBinding != nil {
			b.CosmosDBBinding.DatabaseName = dbName
		}
	}
}

// WithContainer sets the CosmosDB container name.
func WithContainer(containerName string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.CosmosDBBinding != nil {
			b.CosmosDBBinding.ContainerName = containerName
		}
	}
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

// --- ServiceBus Queue-specific options ---

// WithQueueName sets the Service Bus queue name.
func WithQueueName(queueName string) Option {
	return func(rf *RegisteredFunction) {
		if b := rf.triggerBinding(); b != nil && b.ServiceBusBinding != nil {
			b.ServiceBusBinding.QueueName = queueName
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
// Panics if connection is empty — a trigger cannot connect without a setting name.
func WithConnection(connection string) Option {
	if connection == "" {
		panic("WithConnection: connection string setting name must not be empty")
	}
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
