package query

import (
	"github.com/everpan/idig/pkg/entity/meta"
	"testing"
	"xorm.io/builder"
	"xorm.io/xorm/schemas"

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
		want func(values *ColumnValue, err error)
	}{
		{"array vals", `{"cols":["a","b"],"vals":[["a1",2]]}`, func(values *ColumnValue, err error) {
			assert.Nil(t, err)
			assert.Equal(t, 2, len(values.cols))
			assert.Equal(t, 1, len(values.vals))
			assert.Equal(t, 2, len(values.vals[0]))
			assert.Equal(t, 2, int(values.vals[0][1].(float64)))
			bld := builder.Dialect("sqlite3")
			values.BuildInsertSQL(bld, "test")
			sql, _, _ := bld.ToSQL()
			assert.Equal(t, "INSERT INTO test (a,b) Values (?,?)", sql)
			sql2, _ := bld.ToBoundSQL()
			assert.Equal(t, "INSERT INTO test (a,b) Values ('a1',2)", sql2)
		}},
		{"array vals", `{"cols":["a","b"],"vals":[["a1",2],["a1",29]]}`, func(values *ColumnValue, err error) {
			assert.Nil(t, err)
			assert.Equal(t, 2, len(values.cols))
			assert.Equal(t, 2, len(values.vals))
			assert.Equal(t, 2, len(values.vals[0]))
			assert.Equal(t, 29, int(values.vals[1][1].(float64)))
		}},
		{"single", `{"vals":{"a":"va","b":31,"c":"vc"}}`, func(values *ColumnValue, err error) {
			assert.Nil(t, err)
			assert.Equal(t, 1, len(values.vals))
			assert.Equal(t, 3, len(values.vals[0]))
			assert.Equal(t, 3, len(values.cols))
			t.Logf("cols: %v\n", values.cols)
			for i, k := range values.cols {
				if k == "b" {
					assert.Equal(t, 31, int(values.vals[0][i].(float64)))
				}
			}
		}},
		{"multi vals", `{"vals":[{"a":"va","b":31,"c":"vc"},{"a":"va","b":32,"c":"vc"}]}`, func(values *ColumnValue, err error) {
			assert.Nil(t, err)
			assert.Equal(t, 2, len(values.vals))
			assert.Equal(t, 3, len(values.vals[0]))
			assert.Equal(t, 3, len(values.cols))
			t.Logf("cols: %v\n", values.cols)
			for i, k := range values.cols {
				if k == "b" {
					assert.Equal(t, 31, int(values.vals[0][i].(float64)))
				}
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dml := &ColumnValue{}
			dml.Reset()
			err := dml.ParseValues([]byte(tt.data))
			tt.want(dml, err)
		})
	}
}

func TestSubdivisionColumValueToTable(t *testing.T) {
	var (
		m = &meta.Meta{
			ColumnIndex: map[string]*schemas.Column{
				"a": {TableName: "t1"},
				"b": {TableName: "t1"},
				"c": {TableName: "t2"},
				"d": {TableName: "t3"},
				"e": {TableName: "t2"},
				"f": {TableName: "t4"},
			},
		}
		cv = &ColumnValue{
			cols: []string{"a", "b", "c", "d", "e"},
		}
		cv2 = &ColumnValue{
			cols: []string{"a", "b", "c", "d", "e"},
			vals: [][]any{{"1", "2", "3", "4", "5"},
				{"11", "22", "33", "44", "55"},
				{"111", "222", "333", "444", "555"}},
		}
	)
	tests := []struct {
		name string
		m    *meta.Meta
		cv   *ColumnValue
		want func(ret map[string]*ColumnValue, err error)
	}{
		{"none", m, cv, func(ret map[string]*ColumnValue, err error) {
			assert.Nil(t, err)
			t.Logf("ret %v", ret)
			assert.Equal(t, 3, len(ret))
			t.Logf("ret %v", ret["t1"].vals)
			assert.Equal(t, 0, len(ret["t1"].vals))
		}},
		{"3 vals", m, cv2, func(ret map[string]*ColumnValue, err error) {
			assert.Nil(t, err)
			assert.Equal(t, 3, len(ret["t1"].vals))
			assert.Equal(t, 2, len(ret["t1"].vals[0]))
			assert.Equal(t, "222", ret["t1"].vals[2][1].(string))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SubdivisionColumValueToTable(tt.m, tt.cv)
			tt.want(got, err)
		})
	}
}
