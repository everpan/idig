package query

import (
	"fmt"
	"github.com/goccy/go-json"
	"xorm.io/builder"
	"xorm.io/xorm"
)

type UpdateQuery struct {
	Entity   string         `json:"entity"`
	From     string         `json:"from"`
	Wheres   []*Where      `json:"where,omitempty"`
	Vals     map[string]any `json:"vals"` 
	TenantId uint32        `json:"tenant_id,omitempty"`
	engine   *xorm.Engine  `json:"-"`
}

func NewUpdateQuery(tenantId uint32, engine *xorm.Engine) *UpdateQuery {
	return &UpdateQuery{
		TenantId: tenantId,
		engine:   engine,
	}
}

func (q *UpdateQuery) Parse(data []byte) error {
	err := json.Unmarshal(data, q)
	if err != nil {
		return err
	}
	return nil
}

func (q *UpdateQuery) BuildSQL(bld *builder.Builder) error {
	if q.Vals == nil || len(q.Vals) == 0 {
		return fmt.Errorf("no values to update")
	}

	if len(q.Wheres) > 0 {
		err := BuildWheresSQL(bld, q.Wheres)
		if err != nil {
			return fmt.Errorf("build where conditions error: %v", err)
		}
	}

	eq := builder.Eq(q.Vals)
	bld.Update(eq).From(q.From)
	return nil
}
