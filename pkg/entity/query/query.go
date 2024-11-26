package query

import (
	// "encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/goccy/go-json"
	"xorm.io/builder"
	"xorm.io/xorm"
)

type Query struct {
	// Version     string        `json:"version,omitempty"`
	// Entity      string        `json:"entity,omitempty"`
	Alias       string        `json:"alias,omitempty"`
	SelectItems []*SelectItem `json:"select"`
	From        *From         `json:"from"`
	Wheres      []*Where      `json:"where,omitempty"`
	Orders      []*Order      `json:"order,omitempty"`
	Limit       *Limit        `json:"limit,omitempty"`
	TenantId    uint32        `json:"tenant_id,omitempty"`
	engine      *xorm.Engine  `json:"-"`
}

type BuilderSQL interface {
	BuildSQL(bld *builder.Builder) error
}

func NewQuery(tenantId uint32, engine *xorm.Engine) *Query {
	return &Query{
		// Version: "1.0",
		TenantId: tenantId,
		From:     &From{},
		engine:   engine,
	}
}

func (q *Query) NewQuery() *Query {
	return NewQuery(q.TenantId, q.engine)
}

func (q *Query) Parse(data []byte) error {
	qSt := map[string]json.RawMessage{}
	var err error
	err = json.Unmarshal(data, &qSt)
	if err != nil {
		return err
	}
	if _, ok := qSt["select"]; !ok {
		return errors.New(fmt.Sprint("query does not contain select items"))
	}
	if _, ok := qSt["alias"]; ok {
		q.Alias = string(qSt["alias"])
	}
	var errs [5]error
	q.SelectItems, errs[0] = parseSelectItems(qSt["select"])
	q.From, errs[1] = parseFrom(qSt["from"])
	q.Wheres, errs[2] = parseWhere(qSt["where"])
	q.Orders, errs[3] = parseOrder(qSt["order"])
	q.Limit, errs[4] = parseLimit(qSt["limit"])
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func (q *Query) AcquireAllMetas() (map[string]*meta.Meta, error) {
	var metas = map[string]*meta.Meta{}
	for _, ea := range q.From.EntityAlias {
		if ea.Query != nil {
			ms, err := ea.Query.AcquireAllMetas()
			if err != nil {
				return nil, err
			}
			for _, m := range ms {
				metas[m.Entity.EntityName] = m
			}
		} else {
			m, err := meta.AcquireMeta(ea.Entity, q.engine)
			if err != nil {
				return nil, err
			}
			metas[m.Entity.EntityName] = m
		}
	}
	return metas, nil
}

// BuildSQL 构建order/limit/where
func (q *Query) buildCond(bld *builder.Builder) error {
	var err error
	if len(q.Wheres) > 0 {
		for _, w := range q.Wheres {
			err = w.BuildSQL(bld)
			if err != nil {
				return err
			}
		}
	}
	if q.Orders != nil {
		var os []string
		for _, o := range q.Orders {
			os = append(os, o.String())
		}
		bld.OrderBy(strings.Join(os, ","))
	}
	if q.Limit != nil {
		bld.Limit(q.Limit.Num, q.Limit.Offset)
	}
	return nil
}

func (q *Query) buildSelectItems(bld *builder.Builder, m *meta.Meta) *builder.Builder {
	var cols []string
	for _, item := range q.SelectItems {
		cols = append(cols, item.Col)
	}
	tables := m.AttrGroupTableNameFromCols(cols)
	e := m.Entity
	joinCond := fmt.Sprintf("%s.%s = %%s.%s", e.PkAttrTable, e.PkAttrField, e.PkAttrField)
	bld.Select(cols...)
	bld.From(e.PkAttrTable)
	for _, t := range tables {
		bld.LeftJoin(t, fmt.Sprintf(joinCond, t))
	}
	return bld
}

func (q *Query) BuildSQL(bld *builder.Builder) error {
	metas, err := q.AcquireAllMetas()
	if err != nil {
		return err
	}
	for _, ea := range q.From.EntityAlias {
		if ea.Query != nil {
			err1 := ea.Query.BuildSQL(bld)
			if err1 != nil {
				return err1
			}
			return fmt.Errorf("sub query not impl")
		} else {
			if len(q.From.EntityAlias) > 0 {
				return fmt.Errorf("multil-entites not impl")
			}
			entityName := q.From.EntityAlias[0].Entity
			m := metas[entityName]
			q.buildSelectItems(bld, m)
			q.buildCond(bld)
		}
	}
	return nil
}
