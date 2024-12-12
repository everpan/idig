package core

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
	ctxPool = sync.Pool{New: func() interface{} { return &Context{} }}
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
func (c *Context) Tenant() *Tenant {
	return c.tenant
}
func (c *Context) FromFiber(fb *fiber.Ctx) error {
	c.fb = fb
	tenantUid := fb.Get(TenantHeader, DefaultTenant.TenantUid)
	c.tenant = GetFromCache(tenantUid)
	var err error
	if c.tenant == nil {
		c.engine, err = GetEngine(DefaultTenant.Driver, DefaultTenant.DataSource)
	} else {
		c.engine, err = GetEngine(c.tenant.Driver, c.tenant.DataSource)
	}
	return err
}

func IDigHandlerExec(fb *fiber.Ctx, handler IDigHandleFunc) error {
	c := AcquireContext()
	defer ReleaseContext(c)
	err := c.FromFiber(fb)
	if err != nil {
		return err
	}
	return handler(c)
}

// FromFiberOnly 用于轻量化接口
func (c *Context) FromFiberOnly(fb *fiber.Ctx) {
	c.fb = fb
}
