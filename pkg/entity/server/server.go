package server

import (
	"github.com/everpan/idig/pkg/entity/meta"
	"xorm.io/xorm"
)

type Context struct {
	engine *xorm.Engine
}
type Server interface {
	AcquireMeta(entity string) (*meta.Meta, error)
}

func (svr *Context) SetEngine(engine *xorm.Engine) {
	svr.engine = engine
}

func (svr *Context) AcquireMeta(entity string) (*meta.Meta, error) {
	m := meta.GetMetaFromCache(entity)
	if m != nil {
		return m, nil
	}
	var err error
	m, err = meta.GetMetaFromDBAndCached(entity, svr.engine)
	if err != nil {
		return nil, err
	}
	return m, err
}
