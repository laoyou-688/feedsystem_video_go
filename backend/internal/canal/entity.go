package canal

import "time"

type SyncRequest struct {
	Source string     `json:"source"`
	Events []RowEvent `json:"events"`
}

type RowEvent struct {
	EventID   string                   `json:"event_id"`
	Database  string                   `json:"database"`
	Table     string                   `json:"table"`
	Action    string                   `json:"action"`
	OccurredAt time.Time               `json:"occurred_at"`
	Rows      []map[string]interface{} `json:"rows"`
}

type SyncResponse struct {
	Processed int `json:"processed"`
}
