package entity

import (
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
      "user": {
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
    }
  ]
}`, ``},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query{}
			err := q.Parse(tt.str)
			t.Log(err)
			// assert.Contains(t, )
		})
	}
}
