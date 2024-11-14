package core

import (
	"github.com/gofiber/fiber/v2"
	"xorm.io/xorm"
)

type Context struct {
	fb     *fiber.Ctx
	engine *xorm.Engine
	tenant *Tenant
}

var (
	TenantHeader = "X-Tenant-Uid"
)

type IDigHandleFunc func(c *Context) error

func (c *Context) FromFiber(fb *fiber.Ctx) {
	c.fb = fb
	// tenantUid := fb.Get("X-Tenant-Uid","")

}
