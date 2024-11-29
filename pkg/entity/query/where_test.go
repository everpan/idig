package query

import (
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"testing"
	"xorm.io/builder"
)

func TestWhere_parseExpr(t *testing.T) {
	tests := []struct {
		name string
		cond string
		want func(w *Where, err error)
	}{
		{"not expr", `{"col":"a","op":"not expr"}`, func(w *Where, err error) {
			assert.Contains(t, err.Error(), "invalid expression operator")
		}},
		{"val is null", `{"col":"a","op":"expr"}`, func(w *Where, err error) {
			assert.Contains(t, err.Error(), "value is null")
		}},
		{"val type is not map[string]any", `{"col":"a","op":"expr","val":""}`, func(w *Where, err error) {
			assert.Contains(t, err.Error(), "invalid expr value")
		}},
		{"val has no 'sql'", `{"col":"a","op":"expr","val":{"nosql":""}}`, func(w *Where, err error) {
			assert.Contains(t, err.Error(), "invalid expr,has no 'sql'")
		}},
		{"val.sql is not string", `{"col":"a","op":"expr","val":{"sql":0}}`, func(w *Where, err error) {
			assert.Contains(t, err.Error(), "expr.sql type,need string")
		}},
		{"val has no 'args'", `{"col":"a","op":"expr","val":{"sql":"xxx","args-no":""}}`, func(w *Where, err error) {
			assert.Contains(t, err.Error(), "invalid expr,has no 'args'")
		}},
		{"correct expr", `{"col":"a","op":"expr","val":{"sql":"xxx","args":["1",2,"3","4"]}}`, func(w *Where, err error) {
			assert.Nilf(t, err, "want parse ok")
			assert.Equal(t, "3", w.Val.(*builder.Expression).Args()[2])
			// parse again
			e := w.parseExpr()
			assert.Nil(t, e)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Where{}
			err := json.Unmarshal([]byte(tt.cond), w)
			assert.NoError(t, err)
			if err == nil {
				err = w.parseExpr()
				tt.want(w, err)
			}
		})
	}
}
