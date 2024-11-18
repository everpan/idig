package handler

import (
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/entity"
	"github.com/gofiber/fiber/v2"
)

var routes = []*config.IDigRoute{
	{
		Path:    "/entity/meta/:entity",
		Handler: getMeta,
		Method:  fiber.MethodGet,
	},
}

func init() {
	config.RegisterRouter(routes)
}

func getMeta(c *config.Context) error {
	eName := c.Fiber().Params("entity")
	if eName == "" {
		c.SendBadRequestError(fmt.Errorf("no entity specified"))
	}
	meta := entity.GetMetaFromCache(eName)
	var err error
	if meta == nil {
		meta, err = entity.GetMetaFromDBAndCached(eName, c.Engine())
		if err != nil {
			return c.SendBadRequestError(err)
		}
	}
	return c.SendSuccess(meta.ToJMeta())
}