package config

import "xorm.io/xorm"

type InitTableFunT func(engine *xorm.Engine) error

var initTableFunctions []InitTableFunT

func RegisterInitTableFunction(fun InitTableFunT) {
	initTableFunctions = append(initTableFunctions, fun)
}
