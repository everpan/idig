package config

import (
	"sync"
	"xorm.io/xorm"
)

type InitTableFunT func(engine *xorm.Engine) error

var initTableFunctions []InitTableFunT

func RegisterInitTableFunction(fun InitTableFunT) {
	initTableFunctions = append(initTableFunctions, fun)
}

func InitAllTables(engine *xorm.Engine) error {
	for _, initTableFunction := range initTableFunctions {
		err := initTableFunction(engine)
		if err != nil {
			return err
		}
	}
	return nil
}

var (
	engineCache = sync.Map{}
)

func GetEngine(ds string) *xorm.Engine {
	e, ok := engineCache.Load(ds)
	if ok {
		return e.(*xorm.Engine)
	}
	return nil
}

func SetEngine(ds string, engine *xorm.Engine) {
	engineCache.Store(ds, engine)
}
