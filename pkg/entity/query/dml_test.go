package query

import (
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
)

func Test_arrayList(t *testing.T) {
	var v = []any{"string", 1, []string{"a", "b"}}
	s, e := json.Marshal(v)
	assert.Nil(t, e)
	assert.Equal(t, string(s), `["string",1,["a","b"]]`)

	var v1 []any
	json.Unmarshal(s, &v1)
	assert.Equal(t, 1, v[1])
}

func Test_parseValues(t *testing.T) {
	tests := []struct {
		name string
		data string
		want func(values *DmlValues, err error)
	}{
		{"array values", `{"cols":["a","b"],"values":[["a1",2]]}`, func(values *DmlValues, err error) {
			assert.Nil(t, err)
			assert.Equal(t, 2, len(values.cols))
			assert.Equal(t, 1, len(values.values))
			assert.Equal(t, 2, len(values.values[0]))
			assert.Equal(t, 2, int(values.values[0][1].(float64)))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dml := &DmlValues{}
			dml.Reset()
			err := dml.ParseValues([]byte(tt.data))
			tt.want(dml, err)
		})
	}
}
