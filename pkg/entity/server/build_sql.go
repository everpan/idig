package server

import (
	"fmt"
	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/everpan/idig/pkg/entity/query"
	"xorm.io/builder"
)

// GetMetasFromQuery 从查询中获取entity Meta信息
func (svr *Context) GetMetasFromQuery(query *query.Query) (map[string]*meta.Meta, error) {
	var metas = map[string]*meta.Meta{}
	for _, ea := range query.From.EntityAlias {
		if ea.Query != nil {
			ms, err := svr.GetMetasFromQuery(ea.Query)
			if err != nil {
				return nil, err
			}
			for _, m := range ms {
				metas[m.Entity.EntityName] = m
			}
		} else {
			m, err := svr.AcquireMeta(ea.Entity)
			if err != nil {
				return nil, err
			}
			metas[m.Entity.EntityName] = m
		}
	}
	return metas, nil
}

func (svr *Context) BuildSQL(q *query.Query) (*builder.Builder, error) {
	metas, err := svr.GetMetasFromQuery(q)
	if err != nil {
		return nil, err
	}
	bld := builder.Dialect(svr.engine.DriverName())
	for _, ea := range q.From.EntityAlias {
		if ea.Query != nil {
			_, err1 := svr.BuildSQL(ea.Query)
			if err1 != nil {
				return nil, err1
			}
			return nil, fmt.Errorf("sub query not impl")
		} else {
			if len(q.From.EntityAlias) > 0 {
				return nil, fmt.Errorf("multil-entites not impl")
			}
			entityName := q.From.EntityAlias[0].Entity
			m := metas[entityName]
			var items []string
			for _, item := range q.SelectItems {
				items = append(items, item.Col)
			}
			m.JoinAttrsFromColumns(bld, items)
			q.BuildSQL(bld)
			bld.Select(items...)
			// from
			bld.From(q.From.EntityAlias[0].Entity)
			// where
			q.BuildSQL(bld)
		}
	}
	return bld, nil
}
