package query

import (
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_arrayList(t *testing.T) {
	var v = []any{"string", 1, []string{"a", "b"}}
	s, e := json.Marshal(v)
	assert.Nil(t, e)
	assert.Equal(t, string(s), `["string",1,["a","b"]]`)

	var v1 []any
	json.Unmarshal(s, &v1)
	assert.Equal(t, 1, v[1])

	var d []any
	d = append(d, nil)
	assert.Equal(t, 1, len(d))
}

func Test_parseValues(t *testing.T) {
	tests := []struct {
		name string
		data string
		want func(values *ColumnValue, err error)
	}{
		{"array vals 1", `{"cols":["a","b"],"vals":[["a1",2323]]}`, func(cv *ColumnValue, err error) {
			assert.Nil(t, err)
			cv.tableName = "test"
			dt := cv.DataTable()
			assert.Equal(t, 2, len(dt.Columns()))
			assert.Equal(t, 1, len(dt.Values()))
			assert.Equal(t, 2, len(dt.Values()[0]))
			assert.Equal(t, 2323, int(dt.Values()[0][1].(float64)))
			bld := BuildInsertSQL("sqlite3", "test", dt.Columns(), dt.Values()[0])
			sql, _, _ := bld.ToSQL()
			assert.Equal(t, "INSERT INTO test (a,b) Values (?,?)", sql)
			sql2, _ := bld.ToBoundSQL()
			assert.Equal(t, "INSERT INTO test (a,b) Values ('a1',2323)", sql2)
		}},
		{"array vals 2", `{"cols":["pk","a","b"],"vals":[[3,"a1",2]]}`, func(cv *ColumnValue, err error) {
			assert.Nil(t, err)
			cv.tableName = "test"
			dt := cv.DataTable()
			assert.Equal(t, 3, len(dt.Columns()))
			assert.Equal(t, 1, len(dt.Values()))
			assert.Equal(t, 3, len(dt.Values()[0]))
			assert.Equal(t, 2, int(dt.Values()[0][2].(float64)))
			bld := BuildInsertSQL("sqlite3", "test", dt.Columns(), dt.Values()[0])
			sql, _, _ := bld.ToSQL()
			assert.Equal(t, "INSERT INTO test (a,b,pk) Values (?,?,?)", sql)
			sql2, _ := bld.ToBoundSQL()
			assert.Equal(t, "INSERT INTO test (a,b,pk) Values ('a1',2,3)", sql2)
		}},
		{"array vals", `{"cols":["a","b"],"vals":[["a1",2],["a1",29]]}`, func(cv *ColumnValue, err error) {
			assert.Nil(t, err)
			dt := cv.DataTable()
			assert.Equal(t, 2, len(dt.Columns()))
			assert.Equal(t, 2, len(dt.Values()[0]))
			assert.Equal(t, 29, int(dt.Values()[1][1].(float64)))
		}},
		{"single", `{"vals":{"a":"va","b":31,"c":"vc"}}`, func(cv *ColumnValue, err error) {
			assert.Nil(t, err)
			dt := cv.DataTable()
			assert.Equal(t, 1, len(dt.Values()))
			assert.Equal(t, 3, len(dt.Values()[0]))
			assert.Equal(t, 3, len(dt.Columns()))
			for i, k := range dt.Columns() {
				if k == "b" {
					assert.Equal(t, 31, int(dt.Values()[0][i].(float64)))
				}
			}
		}},
		{"multi vals", `{"vals":[{"a":"va","b":31,"c":"vc"},{"a":"va","b":32,"c":"vc"}]}`, func(cv *ColumnValue, err error) {
			assert.Nil(t, err)
			dt := cv.DataTable()
			assert.Equal(t, 3, len(dt.Columns()))
			assert.Equal(t, 3, len(dt.Values()[0]))
			// t.Logf("cols: %v\n", dt.Columns())
			for i, k := range dt.Columns() {
				if k == "b" {
					assert.Equal(t, 31, int(dt.Values()[0][i].(float64)))
				}
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dml := &ColumnValue{}
			err := dml.ParseValues([]byte(tt.data))
			tt.want(dml, err)
		})
	}
}

func TestAddColumn(t *testing.T) {
	dt := NewDataTable()
	assert.Equal(t, 0, dt.AddColumn("column1"))
	assert.Equal(t, 1, dt.AddColumn("column2"))
	assert.Equal(t, 0, dt.AddColumn("column1")) // 重复列
}

func TestAddRow(t *testing.T) {
	dt := NewDataTable()
	dt.AddColumn("column1")
	dt.AddColumn("column2")

	// 添加有效行
	assert.Nil(t, dt.AddRow([]any{"value1", 2}))
	assert.Equal(t, 1, len(dt.Values()))

	// 添加不匹配的行
	assert.NotNil(t, dt.AddRow([]any{"only_one_value"}))
}

func TestFetchRowData(t *testing.T) {
	dt := NewDataTable()
	dt.AddColumn("column1")
	dt.AddRow([]any{"value1"})

	// 测试有效索引
	data, err := dt.FetchRowData(0, []int{0})
	assert.Nil(t, err)
	assert.Equal(t, "value1", data[0])

	// 测试无效索引
	_, err = dt.FetchRowData(1, []int{0})
	assert.NotNil(t, err)
}

func TestValidIndex(t *testing.T) {
	dt := NewDataTable()
	dt.AddColumn("column1")
	dt.AddRow([]any{"value1"})

	// 测试有效索引
	err := dt.ValidIndex([]int{0})
	assert.Nil(t, err)

	// 测试无效索引
	err = dt.ValidIndex([]int{1})
	assert.NotNil(t, err)
}

func TestParseKeyCols(t *testing.T) {
	dt := NewDataTable()
	raw := map[string]any{
		"cols": []any{"column1", "column2"},
	}

	assert.Nil(t, dt.ParseKeyCols(raw))
	assert.Equal(t, 2, len(dt.Columns()))
}

func TestParseKeyVals(t *testing.T) {
	dt := NewDataTable()
	dt.cols = []string{"column1", "column2"}
	raw := map[string]any{
		"vals": []any{
			[]any{"value1", 1},
			[]any{"value2", 2},
		},
	}

	assert.Nil(t, dt.ParseKeyVals(raw))
	assert.Equal(t, 2, len(dt.Values()))
}

/*
func TestSubdivisionColumValueToTable(t *testing.T) {
	var (
		m = &meta.EntityMeta{
			ColumnIndex: map[string]*schemas.Column{
				"pk": {TableName: "t0"},
				"a":  {TableName: "t1"},
				"b":  {TableName: "t1"},
				"c":  {TableName: "t2"},
				"d":  {TableName: "t3"},
				"e":  {TableName: "t2"},
				"f":  {TableName: "t4"},
			},
			Entity: &meta.Entity{PkAttrColumn: "pk"},
		}
		cv = &ColumnValue{
			cols: []string{"a", "b", "c", "d", "e"},
			vals: [][]any{{1, 2, 3, 4, 5}},
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
		m    *meta.EntityMeta
		cv   *ColumnValue
		want func(ret map[string]*ColumnValue, err error)
	}{
		{"none", m, cv, func(ret map[string]*ColumnValue, err error) {
			assert.Nil(t, err)
			assert.Equalf(t, 3, len(ret), "%v", ret)
			assert.Equal(t, 1, ret["t1"].vals[0][1])
		}},
		{"3 vals", m, cv2, func(ret map[string]*ColumnValue, err error) {
			assert.Nil(t, err)
			assert.Equal(t, 3, len(ret["t1"].vals))
			assert.Equal(t, 3, len(ret["t1"].vals[0]))
			assert.Equal(t, "222", ret["t1"].vals[2][2].(string))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SubdivisionColumValueToTable(tt.m, tt.cv)
			tt.want(got, err)
		})
	}
}
*/
