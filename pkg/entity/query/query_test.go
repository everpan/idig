package query

import (
	"github.com/goccy/go-json"
	// "encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQuery_Parse(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		wantErr string
	}{
		{"not has query", "{}", "not found"},
		{"query is not array", `{"query":{}}`, "slice unexpected end of JSON input"},
		{"query user", `{
  "query": [
    {
      "user": {}
    }
  ]
}`, ``},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.str)
			t.Log(err)
			// assert.Contains(t, )
		})
	}
}

/*
{
        "col": [
          "idx",
          "name",
          {
            "col": "age",
            "alias": "nl"
          }
        ],
        "where": [
          {
            "col": "name",
            "op": "eq",
            "val": "ever"
          },
          {
            "col": "age",
            "val": "30",
            "op": "lt",
            "mode": "or"
          }
        ],
        "order": [
          {
            "col": "age",
            "opt": "desc"
          }
        ]
      }
*/

func TestQuery_parseEntityQuery(t *testing.T) {
	tests := []struct {
		name       string
		entityData string
		wantErrStr string
	}{
		{"empty", "", "gf"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query{}
			err := q.parseEntityQuery([]byte(tt.entityData))
			if err != nil {
				assert.Contains(t, err.Error(), tt.wantErrStr)
			}
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
			assert.Contains(t, err.Error(), "slice unexpected end of JSON input")
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
			q := &Query{}
			got, err := q.parseSelectItems(tt.jsonStr)
			tt.wantErr(got, err)
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
		{"tie required", `[{"col":"a","op":"eq"},{"col":"b","op":"lt","tie":""}]`, func(items []*Where, err error) {
			assert.Contains(t, err.Error(), "tie is empty")
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
			q := &Query{}
			got, err := q.parseWhere(tt.jsonStr)
			tt.wantErr(got, err)
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
		{"sub query", `{"other entity":{"select":[]}}`, func(from *From, err error) {
			assert.Nil(t, err)
			assert.Equal(t, "aa", from.EntityAlias[0].Entity)
			assert.Equal(t, "b002", from.EntityAlias[1].Entity)
		}},
	}
	for _, tt := range tests {
		q := &Query{}
		t.Run(tt.name, func(t *testing.T) {
			got, err := q.parseFrom(tt.jsonStr)
			tt.wantErr(got, err)
		})
	}
}

func TestGJson(t *testing.T) {
	tests := []struct {
		name    string
		obj     any
		jsonStr string
	}{
		{"map nil", map[string]any{"a": nil}, `{"a":null}`},
		{"slice nil", []any{}, "[]"},
		{"slice nil", map[string]any{"b": []any{}}, `{"b":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := json.Marshal(&tt.obj)
			assert.Nil(t, err)
			assert.Equal(t, tt.jsonStr, string(d))
		})
	}
}
