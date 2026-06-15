package bindings

import (
	"encoding/json"
	"fmt"
)

// SQLTriggerType is the binding type constant for SQL triggers.
const SQLTriggerType BindingType = "sqlTrigger"

// SQLBinding is the JSON representation for a SQL trigger binding.
type SQLBinding struct {
	TableName               string `json:"tableName"`
	ConnectionStringSetting string `json:"connectionStringSetting"`
}

// SQLOperation enumerates the change types reported by the SQL trigger.
// Values match the Microsoft.Azure.WebJobs.Extensions.Sql SqlChangeOperation
// enum: Insert=0, Update=1, Delete=2.
type SQLOperation int

const (
	SQLOperationInsert SQLOperation = 0
	SQLOperationUpdate SQLOperation = 1
	SQLOperationDelete SQLOperation = 2
)

// String returns a human-readable name for the operation. Unknown values are
// rendered as "SQLOperation(<n>)" to aid debugging without panicking.
func (op SQLOperation) String() string {
	switch op {
	case SQLOperationInsert:
		return "Insert"
	case SQLOperationUpdate:
		return "Update"
	case SQLOperationDelete:
		return "Delete"
	default:
		return fmt.Sprintf("SQLOperation(%d)", int(op))
	}
}

// SQLChange represents a single row change reported by the SQL trigger.
// Item is left as raw JSON so callers can decode into their own row type,
// mirroring CosmosDocument.Data.
type SQLChange struct {
	Operation SQLOperation    `json:"Operation"`
	Item      json.RawMessage `json:"Item"`
}

// SQLTrigger is the user-facing configuration for a SQL trigger.
type SQLTrigger struct {
	Name                    string
	TableName               string
	ConnectionStringSetting string
}

func (s *SQLTrigger) GetBindingType() BindingType { return SQLTriggerType }

func (s *SQLTrigger) ToBinding() Binding {
	return Binding{
		Name:      s.Name,
		Type:      string(s.GetBindingType()),
		Direction: "in",
		SQLBinding: &SQLBinding{
			TableName:               s.TableName,
			ConnectionStringSetting: s.ConnectionStringSetting,
		},
	}
}
