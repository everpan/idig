package entity

import "testing"

func TestSerialMeta(t *testing.T) {
	tests := []struct {
		name    string
		meta    *Meta
		want    string
		wantErr bool
	}{
		{"nil meta", nil, "null", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SerialMeta(tt.meta)
			if (err != nil) != tt.wantErr {
				t.Errorf("SerialMeta() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SerialMeta() got = %v, want %v", got, tt.want)
			}
		})
	}
}
