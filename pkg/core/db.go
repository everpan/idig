package core

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
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

func GetEngine(driver, ds string) (*xorm.Engine, error) {
	e, ok := engineCache.Load(ds)
	if ok {
		return e.(*xorm.Engine), nil
	}
	engine, err := xorm.NewEngine(driver, ds)
	if err != nil {
		return nil, err
	}
	engineCache.Store(ds, engine)
	// 新的db链接，构建基本的数据表
	// 当租户采用隔离db的方式进行管理，每个租户创建的实体将不再是共享的
	// 这可能会带来实施的工作以及数据同步的工作，这个可以后续考虑，暂时先不用考虑
	if err = InitAllTables(engine); err != nil {
		// return nil, err
		// init 发生错误，可能之前已经init过，例如插入了一些数据，导致重复插入出错
		// init table的时机或许要考虑
		logger.Error("init all table error", zap.String("err", err.Error()))
	}
	return engine, nil
}
