package handler

import (
	"fmt"
	"github.com/everpan/idig/pkg/core"

	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/gofiber/fiber/v2"
)

var routes = []*core.IDigRoute{
	{
		Path:    "/entity/meta/:entity",
		Handler: getMeta,
		Method:  fiber.MethodGet,
	},
}

func init() {
	core.RegisterRouter(routes)
}

func getMeta(c *core.Context) error {
	eName := c.Fiber().Params("entity")
	if eName == "" {
		c.SendBadRequestError(fmt.Errorf("no entity specified"))
	}
	m, err := meta.AcquireMeta(eName, c.Engine())
	if err != nil {
		return c.SendBadRequestError(err)
	}
	if m == nil {
		return c.SendBadRequestError(fmt.Errorf("not found meta of entity:%v", eName))
	}
	return c.SendSuccess(m.ToJMeta())
}
