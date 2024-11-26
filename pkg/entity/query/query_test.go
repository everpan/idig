package query

import (
	"github.com/goccy/go-json"
	"xorm.io/builder"

	// "encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery_Parse(t *testing.T) {
	tests := []struct {
		name      string
		str       string
		wantQuery func(*Query, error)
	}{
		{"not has query", "{}", func(query *Query, err error) {
			assert.Contains(t, err.Error(), "query does not contain select items")
		}},
		{"select is not array", `{"select":{}}`, func(query *Query, err error) {
			assert.Contains(t, err.Error(), "slice unexpected end of JSON input")
		}},
		{"empty from", `{"select":[]}`, func(query *Query, err error) {
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "'from' is empty")
		}},
		{"empty select item", `{"select":[],"from":""}`, func(query *Query, err error) {
			assert.Nil(t, err)
			assert.Nil(t, query.SelectItems)
			assert.Equal(t, 0, len(query.SelectItems))
		}},
		{"query user", `{"select":["name","age"],"from":"user","where":[{"col":"name","op":"eq","val":"test"}]}`, func(query *Query, err error) {
			assert.Nil(t, err)
			assert.Equal(t, "name", query.SelectItems[0].Col)
			assert.Equal(t, len(query.Wheres), 1)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(1, nil)
			err := q.Parse([]byte(tt.str))
			tt.wantQuery(q, err)
		})
	}
}

func TestQuery_parseSelectItems(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		wantErr func([]*SelectItem, error)
	}{
		{"err_when_empty", "", func(items []*SelectItem, err error) {
			assert.Contains(t, err.Error(), "unexpected end of JSON input")
		}},
		{"err_when_object", "{}", func(items []*SelectItem, err error) {
			assert.Contains(t, err.Error(), "slice unexpected end of JSON input")
		}},
		{"a1_b", `["a1","b"]`, func(items []*SelectItem, err error) {
			assert.Nil(t, err)
			assert.Equal(t, len(items), 2)
			assert.Equal(t, "a1", items[0].Col)
		}},
		{"a1_b_c1-alias", `["a1","b",{"col":"c1","alias":"cc","opt":"sum(c1)"},{"col":"d"}]`,
			func(items []*SelectItem, err error) {
				assert.Nil(t, err)
				assert.Equal(t, len(items), 4)
				assert.Equal(t, "c1", items[2].Col)
				assert.Equal(t, "cc", items[2].Alias)
				assert.Equal(t, "sum(c1)", items[2].Opt)
				assert.Equal(t, "", items[3].Alias)
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selectItems, err := parseSelectItems([]byte(tt.jsonStr))
			tt.wantErr(selectItems, err)
		})
	}
}

func TestQuery_parseWhere(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		wantErr func([]*Where, error)
	}{
		{"empty", "{}", func(items []*Where, err error) {
			assert.Contains(t, err.Error(), "slice unexpected end of JSON input")
		}},
		{"tie must be and or", `[{"col":"a","op":"eq"},{"col":"b","op":"lt","tie":"not"}]`, func(items []*Where, err error) {
			assert.Contains(t, err.Error(), "'and' or 'or'")
		}},
		{"equal", `[{"col":"a","op":"eq"},{"col":"b","op":"lt","tie":"and"}]`, func(items []*Where, err error) {
			assert.Nil(t, err)
			assert.Equal(t, len(items), 2)
			assert.Equal(t, "eq", items[0].Op)
			assert.Equal(t, "and", items[1].Tie)
			// assert.Contains(t, err.Error(), "slice unexpected end of JSON input")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wheres, err := parseWhere([]byte(tt.jsonStr))
			tt.wantErr(wheres, err)
		})
	}
}

/*
`
	"from":{
		"user":{"alias":"user1"},
		"goods":{
			"select" : ["a1","b1",{"col":"c1","alias":"c2"}],
			"from":"goods",
			"where":[]
         }
	}

`
*/
// TestQuery_parseFrom
func TestQuery_parseFrom(t *testing.T) {
	tests := []struct {
		name    string
		jsonStr string
		wantErr func(*From, error)
	}{
		{"only entity", `"a001"`, func(from *From, err error) {
			assert.Nil(t, err)
			assert.Equal(t, "a001", from.EntityAlias[0].Entity)
		}},
		{"entities", `["a001","b002"]`, func(from *From, err error) {
			assert.Nil(t, err)
			assert.Equal(t, "b002", from.EntityAlias[1].Entity)
		}},
		{"entities", `[{"entity":"aa","alias":"a0"},"b002"]`, func(from *From, err error) {
			assert.Nil(t, err)
			assert.Equal(t, "aa", from.EntityAlias[0].Entity)
			assert.Equal(t, "a0", from.EntityAlias[0].Alias)
			assert.Equal(t, "b002", from.EntityAlias[1].Entity)
		}},
		{"sub query", `{"select":["a","b"],"from":["a","b"]}`, func(from *From, err error) {
			assert.Contains(t, err.Error(), "not impl")
			//assert.Nil(t, err)
			//assert.Equal(t, 1, len(from.EntityAlias))
			//assert.Equal(t, "a", from.EntityAlias[0].Query.From.EntityAlias[0].Entity)
			//assert.Equal(t, "aa", from.EntityAlias[0].Entity)
			//assert.Equal(t, "b002", from.EntityAlias[1].Entity)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, err := parseFrom([]byte(tt.jsonStr))
			tt.wantErr(from, err)
		})
	}
}

func TestGJson(t *testing.T) {
	tests := []struct {
		name    string
		obj     any
		jsonStr string
	}{
		// {"map nil", map[string]any{"a": nil}, `{"a":null}`},
		{"slice nil", []any{}, "[]"},
		// {"slice nil", map[string]any{"b": []any{}}, `{"b":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := json.Marshal(&tt.obj)
			assert.Nil(t, err)
			assert.Equal(t, tt.jsonStr, string(d))
		})
	}
}

func TestQuery_buildCond(t *testing.T) {
	tests := []struct {
		name      string
		queryJSON string
		wantSQL   string
		wantErr   string
	}{
		{"where cond", `{"select":["*"],"from":"t",
"where":[{"col":"a","op":"eq","val":"a-val"},
{"col":"a1","op":"lt","val":"a1-val"},
{"tie":"or","col":"a2","op":"like","val":"jk"}
]}`,
			"SELECT * FROM test WHERE (a=? AND a1<?) OR a2 LIKE ?", ""},
		{"limit", `{"select":["a"],"from":"t",
"where":[{"col":"a","op":"eq","val":"a-v"}],
"limit":{"offset":23,"num":34}}`,
			"SELECT * FROM test WHERE a=? LIMIT 34 OFFSET 23", ""},
		{"order by", `{"select":["a"],"from":"t",
"where":[{"col":"a","op":"eq","val":"a-v"}],
"limit":{"offset":23,"num":34},"order":[{"col":"a","opt":"desc"},{"col":"b"}]}`,
			"SELECT * FROM test WHERE a=? ORDER BY a desc,b asc LIMIT 34 OFFSET 23", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery(0, nil)
			err := q.Parse([]byte(tt.queryJSON))
			if err != nil {
				t.Logf(err.Error())
				assert.Contains(t, err.Error(), tt.wantErr)
			}
			// t.Logf("query %v", q)
			assert.NotNil(t, q)
			bld := builder.Dialect("sqlite3")
			bld.Select("*").From("test")
			q.buildCond(bld)
			sql, args, err := bld.ToSQL()
			if err != nil {
				t.Logf(err.Error())
				assert.Contains(t, err.Error(), tt.wantErr)
			}
			data, err := json.Marshal(q)
			t.Logf("query %v : %v,args:%v", string(data), err, args)
			assert.Equal(t, tt.wantSQL, sql)
		})
	}
}
