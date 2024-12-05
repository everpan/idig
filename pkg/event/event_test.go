package event

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventSerialization(t *testing.T) {
	tests := []struct {
		name    string
		event   Event
		wantErr bool
	}{
		{
			name: "basic event",
			event: Event{
				ID:        "test-1",
				Type:      "test.created",
				Source:    "test_service",
				Data:      map[string]interface{}{"key": "value"},
				Timestamp: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "empty data event",
			event: Event{
				ID:        "test-2",
				Type:      "test.empty",
				Source:    "test_service",
				Data:      map[string]interface{}{},
				Timestamp: time.Now(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test JSON marshaling
			data, err := json.Marshal(tt.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("json.Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Test JSON unmarshaling
			var decoded Event
			err = json.Unmarshal(data, &decoded)
			if (err != nil) != tt.wantErr {
				t.Errorf("json.Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Compare fields
			if decoded.ID != tt.event.ID {
				t.Errorf("Event ID mismatch: got %v, want %v", decoded.ID, tt.event.ID)
			}
			if decoded.Type != tt.event.Type {
				t.Errorf("Event Type mismatch: got %v, want %v", decoded.Type, tt.event.Type)
			}
			if decoded.Source != tt.event.Source {
				t.Errorf("Event Source mismatch: got %v, want %v", decoded.Source, tt.event.Source)
			}
		})
	}
}
