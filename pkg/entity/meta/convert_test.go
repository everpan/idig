package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMeta_Marshal(t *testing.T) {
	tests := []struct {
		name       string
		entityName string
		wantStr    string
	}{
		{"marshal user", "user", `"name":"user_idx"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := getMetaFromDBAndCached(tt.entityName, engine)
			if err != nil {
				t.Error(err)
			}
			got, err := m.Marshal()
			t.Logf("%v", string(got))
			assert.Contains(t, string(got), tt.wantStr)
		})
	}
}
