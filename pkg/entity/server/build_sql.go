package server

import (
	"fmt"
	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/everpan/idig/pkg/entity/query"
	"xorm.io/builder"
)

// GetMetasFromQuery 从查询中获取Meta信息
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

func (svr *Context) BuildSQL(query *query.Query) (*builder.Builder, error) {
	metas, err := svr.GetMetasFromQuery(query)
	if err != nil {
		return nil, err
	}
	b := builder.Dialect(svr.engine.DriverName())
	for _, ea := range query.From.EntityAlias {
		if ea.Query != nil {
			_, err1 := svr.BuildSQL(ea.Query)
			if err1 != nil {
				return nil, err1
			}
			return nil, fmt.Errorf("sub query not impl")
		} else {
			if len(query.From.EntityAlias) > 0 {
				return nil, fmt.Errorf("multil-entites not impl")
			}
			m := metas[query.From.EntityAlias[0].Entity]

			// select
			var items []string
			for _, item := range query.SelectItems {
				if item.Alias != "" {
					items = append(items, item.Col+" as "+item.Alias)
				} else {
					items = append(items, item.Col)
				}
			}
			b.Select(items...)
			// from
			b.From(query.From.EntityAlias[0].Entity)
			// where

		}
	}
}
