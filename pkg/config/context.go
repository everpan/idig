package config

import (
	"github.com/gofiber/fiber/v2"
	"sync"
	"xorm.io/xorm"
)

type Context struct {
	fb     *fiber.Ctx
	engine *xorm.Engine
	tenant *Tenant
}

var (
	TenantHeader = "X-Tenant-Uid"
	ctxPool      = sync.Pool{New: func() interface{} { return &Context{} }}
)

type IDigRoute struct {
	Path    string
	Handler IDigHandleFunc //优先级高
	// FiberHandler fiber.Handler
	Method   string
	Children []*IDigRoute
}

type IDigHandleFunc func(c *Context) error

func AcquireContext() *Context {
	return ctxPool.Get().(*Context)
}

func ReleaseContext(c *Context) {
	ctxPool.Put(c)
}

func (c *Context) Fiber() *fiber.Ctx {
	return c.fb
}
func (c *Context) Engine() *xorm.Engine {
	return c.engine
}
func (c *Context) FromFiber(fb *fiber.Ctx) {
	c.fb = fb
	// tenantUid := fb.Get("X-Tenant-Uid","")

}

func Wrap(fb *fiber.Ctx, handler IDigHandleFunc) error {
	c := AcquireContext()
	defer ReleaseContext(c)
	c.FromFiber(fb)
	return handler(c)
}

// FromFiberOnly 用于轻量化接口
func (c *Context) FromFiberOnly(fb *fiber.Ctx) {
	c.fb = fb
}
