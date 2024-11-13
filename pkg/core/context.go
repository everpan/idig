package core

import "xorm.io/xorm"

type Context struct {
	engine *xorm.Engine
}

type Query interface {
}
