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
				ID:        uint64(time.Now().UnixNano()),
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
				ID:        uint64(time.Now().UnixNano()),
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

func TestEventValidation(t *testing.T) {
	tests := []struct {
		name    string
		event   Event
		wantErr bool
	}{
		{
			name: "valid event",
			event: Event{
				ID:        1,
				Type:      "valid.type",
				Source:    "valid.source",
				Data:      map[string]interface{}{"key": "value"},
				Timestamp: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "zero ID",
			event: Event{
				ID:        0,
				Type:      "valid.type",
				Source:    "valid.source",
				Data:      map[string]interface{}{"key": "value"},
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "empty Type",
			event: Event{
				ID:        1,
				Type:      "",
				Source:    "valid.source",
				Data:      map[string]interface{}{"key": "value"},
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "empty Source",
			event: Event{
				ID:        1,
				Type:      "valid.type",
				Source:    "",
				Data:      map[string]interface{}{"key": "value"},
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "nil Data",
			event: Event{
				ID:        1,
				Type:      "valid.type",
				Source:    "valid.source",
				Data:      nil,
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "zero Timestamp",
			event: Event{
				ID:        1,
				Type:      "valid.type",
				Source:    "valid.source",
				Data:      map[string]interface{}{"key": "value"},
				Timestamp: time.Time{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.event.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
